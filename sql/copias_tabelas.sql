COPY projeto_light.clientes (
    id_cliente,
    nome_cliente,
    cidade,
    estado,
    tipo_cliente,
    data_adesao,
    id_cidade,
    id_estado,
    id_cidade_estado
)
FROM 'C:\\database_final\\database_final.csv'
DELIMITER ',' CSV HEADER ENCODING 'UTF8'; 

COPY projeto_light.dim_localidade (  
    cidade,
    estado,

    id_cidade,
    id_estado,
    id_cidade_estado
)
FROM 'C:\\database_final\\dim_localidade.csv' 
DELIMITER ',' CSV HEADER ENCODING 'UTF8'; 

COPY projeto_light.medicoes_energia (  
    id_medicao,
    id_cliente,
    data_medicao,
    consumo_kwh,
    tipo_medicao
)
FROM 'C:\\database_final\\medicoes_energia_tratado.csv' 
DELIMITER ',' CSV HEADER ENCODING 'UTF8';

COPY projeto_light.ocorrencias_tecnicas(
   id_ocorrencia,
   data_ocorrencia,
   cidade,estado,
   tipo_ocorrencia,
   tempo_reparo_h,
   id_cidade,id_estado,
   id_cidade_estado 
)
FROM 'C:\\database_final\\ocorrencias_tecnicas_tratado.csv'
DELIMITER ',' CSV HEADER ENCODING 'UTF8';

COPY projeto_light.perdas_energia(
   id_perda,
   data_perda,
   estado,
   perda_tecnica_kwh,
   perda_nao_tecnica_kwh 
)
FROM 'C:\\database_final\\perdas_energia_tratado.csv'
DELIMITER ',' CSV HEADER ENCODING 'UTF8';
