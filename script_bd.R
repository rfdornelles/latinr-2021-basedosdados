# LatinR 2021 - Base dos Dados
# Demonstração de uso do pacote

# Referências: ------------------------------------------------------------
# https://basedosdados.github.io/mais/access_data_bq/#primeiros-passos
# https://dbplyr.tidyverse.org/

# Youtube: https://www.youtube.com/watch?v=M9ayiseIjvI&t=250s 

# O que é? ----------------------------------------------------------------

# A Base dos Dados.org reúne dados públicos já limpos e organizados. 
# Ela roda em cima da interface do Google BigQuery e posso utilizá-la para
# consumir e processar dados.

# Google Big Query --------------------------------------------------------

# É um produto do Google dentro das soluções de nuvem (cloud computing).
# Eles "alugam" a interface deles para usuários comuns. 
# O consumo de dados é pago, mas na prática sai de graça pois o limite de dados
# é de 1 TB por mês. Ou seja, é muito difícil bater esse limite.

# Pacote {basedosdados} ---------------------------------------------------

# A BD tem um pacote para o R que facilita todo o processo. Ele vai permitir
# que a gente faça download direto dos dados, que execute a interface SQL ou,
# então, utilize o poder do R para manusear os dados remotamente e apenas baixar
# quando a análise estiver encaminhada.

# Ele está disponível no CRAN:
install.packages("basedosdados")

# Criar uma conta no Google Cloud -----------------------------------------

# https://console.cloud.google.com/projectselector2/home/dashboard
# Devemos ter um project-id para usar com a BD.
# Meu project-id vai ser usado como se fosse uma "comanda".
# 

# Rodando o pacote --------------------------------------------------------

library(basedosdados)

# Vou setar meu billing-id, que é o id do projeto:
set_billing_id("bd-latinr-2021")


# Procurar uma base de interesse ------------------------------------------

# A estrutura dos dados do BD+ é: <entidade>.<tabela>
# Por exemplo: br_senado_cpipandemia.discursos
# Vamos no site escolher uma base:
# https://basedosdados.org/

# Vamos usar:
# dataset: 
# tabela: 

# Download de direto ------------------------------------------------------

# Crio uma query SQL simples:
query <- "
  SELECT * FROM `basedosdados.br_ana_atlas_esgotos.municipio` 
  WHERE sigla_uf = 'AC'
  "
download(query, path = "dados/dados_esgoto.csv")

# Na primeira vez que rodar é esperado que o R ative a interface com o google
# e faça meu login. Se não funcionar:
bigrquery::bq_auth()


# Rodar um comando SQL ----------------------------------------------------

esgotos_acre <- basedosdados::read_sql(query)

# Interface com dplyr -----------------------------------------------------

# Agora a BD também funciona com a interface do {dplyr}, de forma que
# podemos manusear as bases usando os comandos do pacote que já conhecemos!

# O primeiro passo é conectar com a base de interesse com o comando

# Agora, posso realizar as operações que quiser com o objeto
# Contudo, ele está remoto e não no meu computador

# Carregar o tidyverse
library(tidyverse)

# escolher a base
nome_base <- "br_ana_atlas_esgotos.municipio"

base_remota <- basedosdados::bdplyr(nome_base)

base_remota

# nosso objetivo é verificar a população atendida por esgoto nos municipios

base_remota_preparada <- base_remota %>% 
  mutate(
    prop_atendimento = populacao_atendida_2035 / populacao_urbana_2035
  ) %>% 
  select(id_municipio, sigla_uf, prop_atendimento) 


# posso também verificar o que está sendo feito com o comando show_query do
# {dplyr}
dplyr::show_query(base_remota_preparada)

# base auxiliar de municipios

base_auxiliar <- bdplyr("br_bd_diretorios_brasil.municipio")

base_auxiliar <- base_auxiliar %>% 
  select(
    id_municipio, nome, regiao
  )

# juntar as duas bases

base_junta <- base_remota_preparada %>% 
  left_join(base_auxiliar, by = "id_municipio")

# Coletar os dados ou salvar em disco -------------------------------------

# Depois de realizar as operações que quiser, posso "coletar" os dados, ou seja
# mandar baixar do servidor para a memória do meu computador:

base_esgoto <- bd_collect(base_junta)

# Posso também salvar diretamente em disco:

bd_write_rds(base_junta, path = "dados/base_esgoto_preparada.rds")

# Ou usar qualquer outra função de escrita:
# exemplo salvado em .xlsx
bd_write(
  base_junta,
  .write_fn = writexl::write_xlsx,
  path = "dados/base_salva.xlsx"
)

# Outro exemplo -----------------------------------------------------------

# o billing_id já foi setado

# definir a tabela que vou operar:
nome_tabela <- "br_mc_auxilio_emergencial.microdados"

# fazer a conexão remota
base_remota <- bdplyr(nome_tabela)

# ver a cara dela
base_remota

# valor médio do beneficio pago por estado
tabela_auxilio <- base_remota %>% 
  select(mes, sigla_uf, valor_beneficio, enquadramento) %>% 
  group_by(sigla_uf) %>% 
  summarise(
    valor_total = sum(valor_beneficio, na.rm = TRUE),
    qnt_beneficiarios = n()
  ) 

tabela_auxilio <- bd_collect(tabela_auxilio)

tabela_auxilio_coletada <- tabela_auxilio %>% 
  mutate(
    valor_medio = valor_total / qnt_beneficiarios
  ) %>% 
  arrange(-valor_medio) 

tabela_auxilio_coletada


# Ver por municipio -------------------------------------------------------

# valor médio do beneficio pago por estado
tabela_auxilio_muni <- base_remota %>% 
  select(mes, id_municipio, valor_beneficio, enquadramento) %>% 
  group_by(id_municipio) %>% 
  summarise(
    valor_total = sum(valor_beneficio),
    qnt_beneficiarios = n()
  ) 

# coletar o dado

tabela_auxilio_muni <- bd_collect(tabela_auxilio_muni)

# prosseguir a análise
tabela_auxilio_coletada_muni <- tabela_auxilio_muni %>% 
  mutate(
    valor_medio = valor_total / qnt_beneficiarios
  ) %>% 
  arrange(-valor_medio) 

tabela_auxilio_coletada_muni %>% 
  arrange(valor_medio)





