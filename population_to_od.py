from lxml import etree as ET # Usando lxml para melhor desempenho
from math import hypot
import sys
import argparse
import numpy as np # Necessário para scipy.spatial.KDTree
from scipy.spatial import KDTree # Para busca otimizada de nós próximos

# --- Funções Auxiliares (time_to_seconds, find_outgoing_link) ---
# (find_closest_node será substituída pela lógica da KDTree)
def time_to_seconds(time_str):
    """Converte uma string de tempo HH:MM:SS ou HH:MM para segundos desde a meia-noite."""
    if not time_str:
        return None
    try:
        parts = list(map(int, time_str.split(':')))
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            return parts[0] * 3600 + parts[1] * 60
        return None
    except ValueError:
        return None

def find_outgoing_link(node_id_from, links_map_from_node):
    """Encontra o ID de um link de saída do nó especificado."""
    if node_id_from in links_map_from_node and links_map_from_node[node_id_from]:
        return links_map_from_node[node_id_from][0]['id']
    return "UNKNOWN_LINK"

# --- Funções Principais (load_network_data, process_population) ---

def load_network_data(network_file_path):
    """
    Lê o arquivo network.xml, extrai dados de nós, constrói uma KD-Tree
    para busca rápida de nós próximos, e extrai informações de links de saída.
    """
    node_coords_list = [] # Lista de coordenadas [x, y] para construir a KD-Tree
    node_id_map = []      # Lista de IDs de nós, correspondendo à ordem em node_coords_list
    nodes_data_for_kdtree = {} # Para armazenar temporariamente {'id': id, 'x': x, 'y': y} antes de popular as listas
    
    links_from_node_map = {}

    print(f"Iniciando o parsing do arquivo de rede: {network_file_path}", file=sys.stderr)
    try:
        # Usar iterparse para o network.xml
        context = ET.iterparse(network_file_path, events=('end',), tag='node')
        for _, elem in context:
            node_id = elem.get('id')
            x_str = elem.get('x')
            y_str = elem.get('y')
            if node_id and x_str is not None and y_str is not None:
                try:
                    coord_x = float(x_str)
                    coord_y = float(y_str)
                    # Adiciona às listas para a KD-Tree
                    node_coords_list.append([coord_x, coord_y])
                    node_id_map.append(node_id)
                except ValueError:
                    print(f"Aviso: Não foi possível converter coordenadas para o nó {node_id} (x='{x_str}', y='{y_str}'). Pulando nó.", file=sys.stderr)
            elem.clear() # Limpa o elemento <node> da memória
            # Remove o elemento do pai para economizar mais memória com lxml, se necessário
            # while elem.getprevious() is not None:
            #     del elem.getparent()[0]
        
        # Processar links (pode ser necessário um segundo parse ou carregar tudo se o arquivo não for gigante)
        # Se o network.xml for muito grande, esta parte também precisaria de iterparse cuidadoso.
        # Para simplificar, vamos assumir que a parte dos links pode ser processada de forma eficiente
        # ou que o número de links não é o gargalo principal comparado ao iterparse da população.
        # Uma abordagem mais robusta seria fazer dois passes ou usar iterparse com tags múltiplas.
        
        # Segundo iterparse para links (ou um parse completo se o arquivo de rede não for excessivamente grande)
        context_links = ET.iterparse(network_file_path, events=('end',), tag='link')
        for _, elem in context_links:
            link_id = elem.get('id')
            from_node = elem.get('from')
            to_node = elem.get('to')
            if link_id and from_node and to_node:
                if from_node not in links_from_node_map:
                    links_from_node_map[from_node] = []
                links_from_node_map[from_node].append({'id': link_id, 'to': to_node})
            elem.clear()
            # while elem.getprevious() is not None:
            #     del elem.getparent()[0]

    except ET.XMLSyntaxError as e: # lxml usa XMLSyntaxError
        print(f"Erro de sintaxe no XML da rede: {e}", file=sys.stderr)
        return None, None, None
    except FileNotFoundError:
        print(f"Arquivo de rede não encontrado: {network_file_path}", file=sys.stderr)
        return None, None, None
    except Exception as e:
        print(f"Erro inesperado ao carregar a rede: {e}", file=sys.stderr)
        return None, None, None

    if not node_coords_list:
        print("Nenhum nó encontrado no arquivo de rede.", file=sys.stderr)
        return None, None, None
        
    # Constrói a KD-Tree
    print("Construindo KD-Tree para os nós da rede...", file=sys.stderr)
    node_kdtree = KDTree(np.array(node_coords_list))
    print(f"KD-Tree construída. Rede carregada: {len(node_id_map)} nós.", file=sys.stderr)
    
    return node_kdtree, node_id_map, links_from_node_map

def find_closest_node_kdtree(coord_x, coord_y, kdtree, node_id_map_list):
    """Encontra o ID do nó mais próximo usando a KD-Tree."""
    if kdtree is None:
        return None
    # A KDTree.query retorna (distância, índice)
    distance, index = kdtree.query([coord_x, coord_y])
    return node_id_map_list[index]

def process_population(population_file_path, output_trips_file_path, 
                       node_kdtree, node_id_map_list, network_links_from_node):
    """
    Lê o arquivo population.xml de forma incremental, processa as viagens de carro,
    e escreve o arquivo trips.xml. Usa KD-Tree para busca de nós.
    """
    if node_kdtree is None or not node_id_map_list:
        print("Dados da KD-Tree da rede não carregados. Não é possível processar a população.", file=sys.stderr)
        return

    trip_counter = 0
    person_counter = 0
    
    current_person_id = None
    is_selected_plan = False
    plan_items = [] # Armazena itens do plano atual (atividades, pernas)
    
    print(f"Iniciando processamento da população de {population_file_path}...", file=sys.stderr)
    try:
        with open(output_trips_file_path, 'w', encoding='utf-8') as outfile:
            outfile.write("<scsimulator_matrix>\n")

            # iterparse para processamento incremental com lxml
            # Focamos nos eventos 'end' para processar os elementos depois de completos
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
                            plan_items = [] # Reinicia para este plano
                        else:
                            is_selected_plan = False
                
                elif event == 'end': # Processar no 'end' para ter o elemento completo
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
                                    pass # Coordenadas inválidas, será tratado depois
                            
                            plan_items.append({
                                'type': 'activity',
                                'details': {
                                    'coords': current_activity_coords,
                                    'end_time': act_end_time,
                                    'activity_type': act_type 
                                }
                            })
                        elif elem.tag == 'leg':
                            # Coleta todos os atributos da perna
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

                                                    # Usa KD-Tree para encontrar nós próximos
                                                    origin_node_id = find_closest_node_kdtree(orig_x, orig_y, node_kdtree, node_id_map_list)
                                                    destination_node_id = find_closest_node_kdtree(dest_x, dest_y, node_kdtree, node_id_map_list)

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
                        # Limpeza após processar o plano
                        is_selected_plan = False
                        plan_items = []
                        elem.clear()
                        # Para lxml, também é bom remover o elemento do pai para garantir a liberação de memória
                        # Se elem.getparent() não for None:
                        #    elem.getparent().remove(elem)
                        # Contudo, iterparse com elem.clear() é geralmente o principal.

                    elif elem.tag == 'person':
                        current_person_id = None # Reseta
                        elem.clear()
                        # if elem.getparent() is not None:
                        #    elem.getparent().remove(elem)
            
            outfile.write("</scsimulator_matrix>\n")
            sys.stdout.flush()
            sys.stderr.flush()
            print(f"\nProcessamento concluído. Geradas {trip_counter} viagens para {person_counter} pessoas.", file=sys.stderr)

    except FileNotFoundError:
        print(f"Arquivo de população não encontrado: {population_file_path}", file=sys.stderr)
    except ET.XMLSyntaxError as e: # lxml usa XMLSyntaxError
        print(f"Erro de sintaxe no XML da população: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Um erro inesperado ocorreu durante o processamento da população: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)


# --- Bloco de Execução Principal ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Processa arquivos population.xml e network.xml do MatSim para gerar um arquivo trips.xml. Otimizado para velocidade."
    )
    parser.add_argument("population_file", help="Caminho para o arquivo population.xml (entrada)")
    parser.add_argument("network_file", help="Caminho para o arquivo network.xml (entrada)")
    parser.add_argument("output_file", help="Caminho para o arquivo trips.xml a ser gerado (saída)")

    args = parser.parse_args()

    POPULATION_XML_FILE = args.population_file
    NETWORK_XML_FILE = args.network_file
    OUTPUT_TRIPS_XML_FILE = args.output_file

    print(f"Carregando dados da rede de: {NETWORK_XML_FILE}", file=sys.stderr)
    kdtree, node_ids, links_from_node = load_network_data(NETWORK_XML_FILE)
    
    if kdtree is not None and node_ids is not None and links_from_node is not None:
        print(f"Iniciando processamento da população de: {POPULATION_XML_FILE}", file=sys.stderr)
        print(f"A saída será escrita em: {OUTPUT_TRIPS_XML_FILE}", file=sys.stderr)
        process_population(POPULATION_XML_FILE, OUTPUT_TRIPS_XML_FILE, kdtree, node_ids, links_from_node)
        print("Script finalizado.", file=sys.stderr)
    else:
        print("Falha ao carregar os dados da rede. O processamento da população foi abortado.", file=sys.stderr)