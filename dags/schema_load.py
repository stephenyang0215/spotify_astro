import json
def dfs(parent_node_lst, graph, node, parent_node = ''):  #function for dfs 
  if parent_node!='.'+node:
    parent_node = parent_node+'.'+node
    parent_node_lst.append(parent_node[1:])
  if type(graph) == dict: 
    for key in graph.keys():
      dfs(parent_node_lst, graph[key], key, parent_node)
  elif type(graph) == list:
    if type(graph[0])==dict:
      for key in graph[0].keys():
        mark.append(parent_node)
        dfs(parent_node_lst, graph[0][key], key, parent_node)

def json_schema_auto(file_path, table):
  file = open(file_path)
  data = json.load(file)
  parent_node_lst = list()
  global mark
  mark = []
  distinct_parent_node = []
  mark_distinct  = []
  distinct_lst = []
  for node in list(data.keys()):
    parent_node = ''
    dfs(parent_node_lst, data[node], node)#Pick first one key of the json object
  [distinct_parent_node.append(element) for element in parent_node_lst if element not in distinct_parent_node]
  [mark_distinct.append(node) for node in mark if node not in mark_distinct]
  for idx in range(len(mark_distinct)):
    if '.' in mark_distinct[idx]:
      mark_distinct[idx] = mark_distinct[idx][1:]
  
  for node in mark_distinct:
    if '.' not in node:
      distinct_lst.append(node.upper())
    else:
      distinct_lst.append(node.upper())
  flat_lst = []
  for node in distinct_lst:
    if '.' not in node:
      flat_lst.append(f'LATERAL FLATTEN(input => '+node.split('_')[-1]+') '+node+',')
    else:
      if node.count('.')==1:
        flat_lst.append(f'LATERAL FLATTEN(input => '+'_'.join(node.split('.')[:-1])+':'+node.split('.')[-1].lower()+') '+node.replace('.','_')+',')
      else:
        flat_lst.append(f'LATERAL FLATTEN(input => '+'_'.join(node.split('.')[:-1])+'.value:'+node.split('.')[-1].lower()+') '+node.replace('.','_')+',')
  flat_lst[-1]=flat_lst[-1][:-1]
  mark_distinct.sort(key=len, reverse=True)

  statement = []
  distinct_lst = []
  state = ''
  for element in distinct_parent_node:
    if '.' in element:
      for mark in mark_distinct:
        if mark in element:
          flat_name = element.split(mark)[-1].replace('.',':').lower()
          alias = element.replace('.','_').upper()
          statement.append(mark.replace('.','_').upper()+f'.VALUE{flat_name}::string AS {alias},')
          state = 'break'
          break
      if state == 'break':
        state = ''
        continue
      element = element.replace('.',':')
      statement.append(element+' AS '+element.replace(':', '_').upper()+',')
    else:
      statement.append(element+',')
  statement[-1] = statement[-1][:-1]
  snowflake_sql_statement = 'SELECT \n'+'\n'.join(statement)+'\nFROM '+"{{source('spotify', '"+table.lower()+"')}}"+',\n'+'\n'.join(flat_lst)
  print(snowflake_sql_statement)
  return snowflake_sql_statement