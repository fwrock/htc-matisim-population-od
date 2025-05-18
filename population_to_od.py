import xml.etree.ElementTree as ET
from math import hypot # Para calcular distância Euclidiana (hypot(dx, dy) é sqrt(dx*dx + dy*dy))
import sys
import argparse # Importa o módulo para lidar com argumentos de linha de comando

# --- Funções Auxiliares (time_to_seconds, find_closest_node, find_outgoing_link) ---
# (Estas funções permanecem as mesmas do script anterior)
def time_to_seconds(time_str):
    """Converte uma string de tempo HH:MM:SS ou HH:MM para segundos desde a meia-noite."""
    if not time_str:
        return None
    try:
        parts = list(map(int, time_str.split(':')))
        if len(parts) == 3:  # HH:MM:SS
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:  # HH:MM
            return parts[0] * 3600 + parts[1] * 60
        return None
    except ValueError:
        return None

def find_closest_node(coord_x, coord_y, nodes_map):
    """Encontra o ID do nó mais próximo de um dado par de coordenadas (x, y)."""
    if not nodes_map:
        return None
    min_dist_sq = float('inf')
    closest_node_id = None
    for node_id, node_attrs in nodes_map.items():
        dx = coord_x - node_attrs['x']
        dy = coord_y - node_attrs['y']
        dist_sq = dx*dx + dy*dy
        if dist_sq < min_dist_sq:
            min_dist_sq = dist_sq
            closest_node_id = node_id
    return closest_node_id

def find_outgoing_link(node_id_from, links_map_from_node):
    """Encontra o ID de um link de saída do nó especificado."""
    if node_id_from in links_map_from_node and links_map_from_node[node_id_from]:
        return links_map_from_node[node_id_from][0]['id']
    return "UNKNOWN_LINK"

# --- Funções Principais (load_network_data, process_population) ---
# (Estas funções permanecem as mesmas do script anterior,
#  apenas recebem os caminhos dos arquivos como parâmetros)

def load_network_data(network_file_path):
    """Lê o arquivo network.xml e extrai dados de nós e informações de links de saída."""
    nodes = {}
    links_from_node_map = {}

    print(f"Iniciando o parsing do arquivo de rede: {network_file_path}", file=sys.stderr)
    try:
        context = ET.iterparse(network_file_path, events=('end',))
        for _, elem in context:
            if elem.tag == 'node':
                node_id = elem.get('id')
                x_str = elem.get('x')
                y_str = elem.get('y')
                if node_id and x_str is not None and y_str is not None:
                    try:
                        nodes[node_id] = {'x': float(x_str), 'y': float(y_str)}
                    except ValueError:
                        print(f"Aviso: Não foi possível converter coordenadas para o nó {node_id} (x='{x_str}', y='{y_str}'). Pulando nó.", file=sys.stderr)
                elem.clear()
            elif elem.tag == 'link':
                link_id = elem.get('id')
                from_node = elem.get('from')
                to_node = elem.get('to')
                if link_id and from_node and to_node:
                    if from_node not in links_from_node_map:
                        links_from_node_map[from_node] = []
                    links_from_node_map[from_node].append({'id': link_id, 'to': to_node})
                elem.clear()
    except ET.ParseError as e:
        print(f"Erro de parsing no XML da rede: {e}", file=sys.stderr)
        return None, None
    except FileNotFoundError:
        print(f"Arquivo de rede não encontrado: {network_file_path}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"Erro inesperado ao carregar a rede: {e}", file=sys.stderr)
        return None, None
        
    print(f"Rede carregada: {len(nodes)} nós e informações de links de saída para {len(links_from_node_map)} nós.", file=sys.stderr)
    return nodes, links_from_node_map

def process_population(population_file_path, output_trips_file_path, network_nodes, network_links_from_node):
    """
    Lê o arquivo population.xml de forma incremental, processa as viagens de carro,
    e escreve o arquivo trips.xml.
    """
    if not network_nodes:
        print("Dados da rede não carregados. Não é possível processar a população.", file=sys.stderr)
        return

    trip_counter = 0
    person_counter = 0
    
    current_person_id = None
    is_selected_plan = False
    plan_items = []
    
    print(f"Iniciando processamento da população de {population_file_path}...", file=sys.stderr)
    try:
        with open(output_trips_file_path, 'w', encoding='utf-8') as outfile:
            outfile.write("<scsimulator_matrix>\n")

            context = ET.iterparse(population_file_path, events=('start', 'end'))
            
            for event, elem in context:
                if event == 'start':
                    if elem.tag == 'person':
                        current_person_id = elem.get('id')
                        person_counter += 1
                        if person_counter % 100000 == 0:
                            print(f"Processando pessoa {person_counter} (ID: {current_person_id})", file=sys.stderr)
                    elif elem.tag == 'plan' and current_person_id is not None:
                        if elem.get('selected') == 'yes':
                            is_selected_plan = True
                            plan_items = []
                        else:
                            is_selected_plan = False

                elif event == 'end':
                    if is_selected_plan:
                        if elem.tag == 'activity':
                            act_x_str = elem.get('x')
                            act_y_str = elem.get('y')
                            act_end_time = elem.get('end_time')
                            act_type = elem.get('type')

                            current_activity_coords = None
                            if act_x_str is not None and act_y_str is not None:
                                try:
                                    current_activity_coords = (float(act_x_str), float(act_y_str))
                                except ValueError:
                                    pass
                            
                            plan_items.append({
                                'type': 'activity',
                                'details': {
                                    'coords': current_activity_coords,
                                    'end_time': act_end_time,
                                    'activity_type': act_type 
                                }
                            })
                        elif elem.tag == 'leg':
                            plan_items.append({'type': 'leg', 'attrs': dict(elem.attrib)})

                    if elem.tag == 'plan' and current_person_id is not None:
                        if is_selected_plan:
                            activity_before_leg_details = None
                            for i, item in enumerate(plan_items):
                                if item['type'] == 'activity':
                                    activity_before_leg_details = item['details']
                                elif item['type'] == 'leg' and activity_before_leg_details is not None:
                                    leg_attrs = item['attrs']
                                    if leg_attrs.get('mode') == 'car':
                                        if (i + 1) < len(plan_items) and plan_items[i+1]['type'] == 'activity':
                                            activity_after_leg_details = plan_items[i+1]['details']
                                            
                                            if activity_before_leg_details['coords'] and activity_after_leg_details['coords']:
                                                start_time_str = leg_attrs.get('dep_time')
                                                if not start_time_str:
                                                    start_time_str = activity_before_leg_details.get('end_time')
                                                
                                                start_seconds = time_to_seconds(start_time_str)

                                                if start_seconds is not None:
                                                    orig_x, orig_y = activity_before_leg_details['coords']
                                                    dest_x, dest_y = activity_after_leg_details['coords']

                                                    origin_node_id = find_closest_node(orig_x, orig_y, network_nodes)
                                                    destination_node_id = find_closest_node(dest_x, dest_y, network_nodes)

                                                    if origin_node_id and destination_node_id:
                                                        link_origin_id = find_outgoing_link(origin_node_id, network_links_from_node)
                                                        
                                                        trip_counter += 1
                                                        trip_name = f"t{trip_counter}"
                                                        
                                                        trip_xml_str = (
                                                            f'  <trip name="{trip_name}" '
                                                            f'origin="{origin_node_id}" '
                                                            f'destination="{destination_node_id}" '
                                                            f'link_origin="{link_origin_id}" '
                                                            f'count="1" '
                                                            f'start="{start_seconds}" '
                                                            f'mode="car" '
                                                            f'digital_rails_capable="false"/>\n'
                                                        )
                                                        outfile.write(trip_xml_str)
                        is_selected_plan = False
                        plan_items = []
                        elem.clear()

                    elif elem.tag == 'person':
                        current_person_id = None
                        elem.clear()
            
            outfile.write("</scsimulator_matrix>\n")
            sys.stdout.flush()
            sys.stderr.flush()
            print(f"\nProcessamento concluído. Geradas {trip_counter} viagens para {person_counter} pessoas.", file=sys.stderr)

    except FileNotFoundError:
        print(f"Arquivo de população não encontrado: {population_file_path}", file=sys.stderr)
    except ET.ParseError as e:
        print(f"Erro de parsing no XML da população: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Um erro inesperado ocorreu durante o processamento da população: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)


# --- Bloco de Execução Principal ---
if __name__ == "__main__":
    # Configura o parser de argumentos da linha de comando
    parser = argparse.ArgumentParser(
        description="Processa arquivos population.xml e network.xml do MatSim para gerar um arquivo trips.xml."
    )
    parser.add_argument(
        "population_file", 
        help="Caminho para o arquivo population.xml (entrada)"
    )
    parser.add_argument(
        "network_file", 
        help="Caminho para o arquivo network.xml (entrada)"
    )
    parser.add_argument(
        "output_file", 
        help="Caminho para o arquivo trips.xml a ser gerado (saída)"
    )

    # Parseia os argumentos fornecidos
    args = parser.parse_args()

    # Usa os caminhos dos arquivos fornecidos como argumentos
    POPULATION_XML_FILE = args.population_file
    NETWORK_XML_FILE = args.network_file
    OUTPUT_TRIPS_XML_FILE = args.output_file

    print(f"Carregando dados da rede de: {NETWORK_XML_FILE}", file=sys.stderr)
    net_nodes, net_links_from_node = load_network_data(NETWORK_XML_FILE)
    
    if net_nodes and net_links_from_node:
        print(f"Iniciando processamento da população de: {POPULATION_XML_FILE}", file=sys.stderr)
        print(f"A saída será escrita em: {OUTPUT_TRIPS_XML_FILE}", file=sys.stderr)
        process_population(POPULATION_XML_FILE, OUTPUT_TRIPS_XML_FILE, net_nodes, net_links_from_node)
        print("Script finalizado.", file=sys.stderr)
    else:
        print("Falha ao carregar os dados da rede. O processamento da população foi abortado.", file=sys.stderr)