CREATE SCHEMA IF NOT EXISTS projeto_light;

-- 2️⃣ Cria o schema
CREATE SCHEMA IF NOT EXISTS projeto_light AUTHORIZATION CURRENT_USER;

-- 3️⃣ Tabela de clientes
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

-- 4️⃣ Dimensão de localidade
CREATE TABLE IF NOT EXISTS projeto_light.dim_localidade (
    id_localidade SERIAL PRIMARY KEY,
    cidade VARCHAR(100),
    estado VARCHAR(100),
    id_cidade INT,
    id_estado INT,
    id_cidade_estado INT
);

-- 5️⃣ Tabela de perdas de energia
CREATE TABLE IF NOT EXISTS projeto_light.perdas_energia (
    id_perda SERIAL PRIMARY KEY,
    data_perda DATE,
    estado VARCHAR(100),
    perda_tecnica_kwh NUMERIC(15,2),
    perda_nao_tecnica_kwh NUMERIC(15,2),
    id_estado INT
);

-- 6️⃣ Tabela de ocorrências técnicas
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

-- 7️⃣ Tabela de medições de energia
CREATE TABLE IF NOT EXISTS projeto_light.medicoes_energia (
    id_medicao SERIAL PRIMARY KEY,
    id_cliente INT,
    data_medicao DATE,
    consumo_kwh NUMERIC(15,2),
    tipo_medicao VARCHAR(50),
    FOREIGN KEY (id_cliente) REFERENCES projeto_light.clientes(id_cliente)
);

