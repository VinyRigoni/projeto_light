CREATE SCHEMA IF NOT EXISTS projeto_light;

--Cria o schema
CREATE SCHEMA IF NOT EXISTS projeto_light AUTHORIZATION CURRENT_USER;

--Tabela dimensão para clientes
CREATE TABLE IF NOT EXISTS projeto_light.clientes (
    id_cliente SERIAL PRIMARY KEY,
    nome_cliente VARCHAR(255),
    cidade VARCHAR(100),
    estado VARCHAR(100),
    tipo_cliente VARCHAR(50),
    data_adesao DATE,
    id_cidade INT,
    id_estado INT,
    id_cidade_estado INT
);

--Tabela dimensão para localidade
CREATE TABLE IF NOT EXISTS projeto_light.dim_localidade (
    id_localidade SERIAL PRIMARY KEY,
    cidade VARCHAR(100),
    estado VARCHAR(100),
    id_cidade INT,
    id_estado INT,
    id_cidade_estado INT
);

--Tabela fato para perdas de energia
CREATE TABLE IF NOT EXISTS projeto_light.perdas_energia (
    id_perda SERIAL PRIMARY KEY,
    data_perda DATE,
    estado VARCHAR(100),
    perda_tecnica_kwh NUMERIC(15,2),
    perda_nao_tecnica_kwh NUMERIC(15,2),
    id_estado INT
);

--Tabela fato para ocorrências técnicas
CREATE TABLE IF NOT EXISTS projeto_light.ocorrencias_tecnicas (
    id_ocorrencia SERIAL PRIMARY KEY,
    data_ocorrencia DATE,
    cidade VARCHAR(100),
    estado VARCHAR(100),
    tipo_ocorrencia VARCHAR(100),
    tempo_reparo_h NUMERIC(10,2),
    id_cidade INT,
    id_estado INT,
    id_cidade_estado INT
);

--Tabela fato para medições de energia
CREATE TABLE IF NOT EXISTS projeto_light.medicoes_energia (
    id_medicao SERIAL PRIMARY KEY,
    id_cliente INT,
    data_medicao DATE,
    consumo_kwh NUMERIC(15,2),
    tipo_medicao VARCHAR(50),
    FOREIGN KEY (id_cliente) REFERENCES projeto_light.clientes(id_cliente)
);

/* ----Tabela para Calendario
CREATE TABLE IF NOT EXISTS projeto_light.dim_calendario (
    data DATE PRIMARY KEY,
    ano INTEGER,
    mes INTEGER,
    dia INTEGER,
    nome_mes VARCHAR(20),
    nome_mes_abrev VARCHAR(3),
    trimestre INTEGER,
    semestre INTEGER,
    nome_dia_semana VARCHAR(20),
    nome_dia_semana_abrev VARCHAR(3),
    numero_dia_semana INTEGER,
    eh_final_de_semana BOOLEAN,
    ano_mes VARCHAR(7)
);

DO
$$
DECLARE
    data_atual DATE := '2020-01-01';
    data_final DATE := '2030-12-31';
BEGIN
    WHILE data_atual <= data_final LOOP
        INSERT INTO projeto_light.dim_calendario (
            data,
            ano,
            mes,
            dia,
            nome_mes,
            nome_mes_abrev,
            trimestre,
            semestre,
            nome_dia_semana,
            nome_dia_semana_abrev,
            numero_dia_semana,
            eh_final_de_semana,
            ano_mes
        )
        VALUES (
            data_atual,
            EXTRACT(YEAR FROM data_atual)::INT,
            EXTRACT(MONTH FROM data_atual)::INT,
            EXTRACT(DAY FROM data_atual)::INT,
            INITCAP(TO_CHAR(data_atual, 'TMMonth', 'lc_time=pt_BR')), 
            INITCAP(TO_CHAR(data_atual, 'Mon', 'lc_time=pt_BR')),
            EXTRACT(QUARTER FROM data_atual)::INT,
            CASE WHEN EXTRACT(MONTH FROM data_atual) <= 6 THEN 1 ELSE 2 END,
            INITCAP(TO_CHAR(data_atual, 'TMDay', 'lc_time=pt_BR')), 
            INITCAP(TO_CHAR(data_atual, 'Dy', 'lc_time=pt_BR')),
            EXTRACT(ISODOW FROM data_atual)::INT,
            CASE WHEN EXTRACT(ISODOW FROM data_atual) IN (6,7) THEN TRUE ELSE FALSE END,
            TO_CHAR(data_atual, 'YYYY-MM')
        );

        data_atual := data_atual + INTERVAL '1 day';
    END LOOP;
END
$$;
*/