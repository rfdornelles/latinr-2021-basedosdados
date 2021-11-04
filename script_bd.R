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

# Vamos no site escolher uma base:
# https://basedosdados.org/

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

# filtar a base

base_remota_preparada <- base_remota %>% 
  filter(silga_uf == "AC")
 
# posso também verificar o que está sendo feito com o comando show_query do
# {dplyr}
dplyr::show_query(base_remota_preparada)

# Coletar os dados ou salvar em disco -------------------------------------

# Depois de realizar as operações que quiser, posso "coletar" os dados, ou seja
# mandar baixar do servidor para a memória do meu computador:

base_esgoto <- bd_collect(base_remota_preparada)

# Posso também salvar diretamente em disco:

bd_write_rds(base_remota_preparada, path = "dados/base_esgoto_preparada.rds")

# Ou usar qualquer outra função de escrita:
# exemplo salvado em .xlsx
bd_write(
  base_remota_preparada,
  .write_fn = writexl::write_xlsx,
  path = "dados/base_salva.xlsx"
)

# Outro exemplo -----------------------------------------------------------

# auxilio emergencial - 257.193.121 de linhas

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

tabela_auxilio_coletada





