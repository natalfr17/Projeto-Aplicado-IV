# Define the subset of columns to read
SELECTED_COLUMNS = [
    "NU_ANO_CENSO",  # Ano de referência do Censo da Educação Superior
    "NO_REGIAO",  # Nome da região geográfica da sede administrativa ou reitoria da IES
    "CO_REGIAO",  # Código da região geográfica da sede administrativa ou reitoria da IES
    "NO_UF",  # Nome da Unidade da Federação da sede administrativa ou reitoria da IES
    "SG_UF",  # Sigla da Unidade da Federação da sede administrativa ou reitoria da IES
    "CO_UF",  # Código da Unidade da Federação da sede administrativa ou reitoria da IES
    "NO_MUNICIPIO",  # Nome do Município da sede administrativa ou reitoria da IES
    "CO_MUNICIPIO",  # Código do Município da sede administrativa ou reitoria da IES
    "TP_ORGANIZACAO_ACADEMICA",  # Tipo de Organização Acadêmica da IES
    "TP_REDE",  # Rede de Ensino
    "TP_CATEGORIA_ADMINISTRATIVA",  # Tipo de Categoria Administrativa da IES
    "CO_IES",  # Código único de identificação da IES
    "NO_CURSO",  # Nome do curso
    "CO_CURSO",  # Código do curso
    "NO_CINE_ROTULO",  # Nome do rótulo CINE
    "CO_CINE_ROTULO",  # Código do rótulo CINE
    "CO_CINE_AREA_GERAL",  # Código da área geral CINE
    "NO_CINE_AREA_GERAL",  # Nome da área geral CINE
    "CO_CINE_AREA_ESPECIFICA",  # Código da área específica CINE
    "NO_CINE_AREA_ESPECIFICA",  # Nome da área específica CINE
    "CO_CINE_AREA_DETALHADA",  # Código da área detalhada CINE
    "NO_CINE_AREA_DETALHADA",  # Nome da área detalhada CINE
    "TP_GRAU_ACADEMICO",  # Tipo de grau acadêmico
    "IN_GRATUITO",  # Informa se o curso é gratuito
    "TP_MODALIDADE_ENSINO",  # Tipo de modalidade de ensino
    "TP_NIVEL_ACADEMICO",  # Tipo de nível acadêmico
]

# Define column mappings (move to a config file if needed)
MAIN_TABLE_COLUMNS = [
    "NU_ANO_CENSO",
    "CO_REGIAO",
    "CO_UF",
    "CO_MUNICIPIO",
    "CO_IES",
    "CO_CURSO",
]

# Define table mappings for normalized data
TABLE_MAPPINGS = {
    "regions": ["CO_REGIAO", "NO_REGIAO"],
    "states": ["CO_UF", "NO_UF", "SG_UF"],
    "municipalities": ["CO_MUNICIPIO", "NO_MUNICIPIO", "CO_UF"],
    "institutions": [
        "CO_IES",
        "TP_ORGANIZACAO_ACADEMICA",
        "TP_REDE",
        "TP_CATEGORIA_ADMINISTRATIVA",
    ],
    "courses": [
        "CO_CURSO",
        "NO_CURSO",
        "CO_CINE_ROTULO",
        "CO_CINE_AREA_GERAL",
        "CO_CINE_AREA_ESPECIFICA",
        "CO_CINE_AREA_DETALHADA",
        "TP_GRAU_ACADEMICO",
        "IN_GRATUITO",
        "TP_MODALIDADE_ENSINO",
        "TP_NIVEL_ACADEMICO",
    ],
    "cine_rotulos": ["CO_CINE_ROTULO", "NO_CINE_ROTULO"],
    "cine_areas_gerais": ["CO_CINE_AREA_GERAL", "NO_CINE_AREA_GERAL"],
    "cine_areas_especificas": [
        "CO_CINE_AREA_ESPECIFICA",
        "NO_CINE_AREA_ESPECIFICA",
    ],
    "cine_areas_detalhadas": ["CO_CINE_AREA_DETALHADA", "NO_CINE_AREA_DETALHADA"],
}


# Define default chunk size for processing
DEFAULT_CHUNK_SIZE = 50000
