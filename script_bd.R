# Curso Equalitas - Introdução ao R
# Aula 05 - 07/10/2021
# Começando com a Base dos Dados

# Nosso material ----------------------------------------------------------

# https://drive.google.com/drive/folders/1R5FpScbMOSRF0VCsBZdjJuGz3bf26g1h?usp=sharing

# Obs.: Esse script é um compilado de conteúdo utilizando material
# disponibilizado pela Curso-R (http://curso-r.com) e de scripts elaborados
# pelos professores Jonatas Varella, Fernando Meireles e Neylson Crepalde.

# Como utilizar esse script? ----------------------------------------------

# Ao invés de utilizar slides, faremos tudo aqui pelo R. Vá acompanhando
# e acrescentando suas próprias anotações. Lembre-se de utilizar # antes
# de cada linha para que o R a ignore ou então coloque a # após o comando.

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
# equalitasdornelles1008

# vamos trabalhar com o Atlas do Esgoto: https://basedosdados.org/dataset/e438dc92-b97c-4e48-ab72-153bf4cf73cc
# Rodando o pacote --------------------------------------------------------

library(tidyverse)
library(basedosdados)


# Vou setar meu billing-id, que é o id do projeto:
set_billing_id("equalitasdornelles1008")


# Procurar uma base de interesse ------------------------------------------

# A estrutura dos dados do BD+ é: <entidade>.<tabela>
# Por exemplo: br_senado_cpipandemia.discursos
# Vamos no site escolher uma base:
# basedosdados.br_inep_censo_escolar.escola

# Vamos usar:
# dataset: br_ana_atlas_esgotos
# tabela: municipio

# Na primeira vez que rodar é esperado que o R ative a interface com o google
# e faça meu login. Se não funcionar:
bigrquery::bq_auth()

# Download de direto ------------------------------------------------------

basedosdados::download(query = "
         SELECT * FROM `basedosdados.br_ana_atlas_esgotos.municipio` 
         WHERE sigla_uf = 'AC'
         ", 
         path =  "dados/esgotos_exemplo.csv")

# Rodar um comando SQL ----------------------------------------------------

query <- "
  SELECT * FROM `basedosdados.br_ana_atlas_esgotos.municipio` 
  WHERE sigla_uf = 'AC'
  "

esgostos_acre <- basedosdados::read_sql(query)

base_ac <- read_sql(query) # lembre de salvar em um objeto

# Interface com dplyr -----------------------------------------------------

# Agora a BD também funciona com a interface do {dplyr}, de forma que
# podemos manusear as bases usando os comandos do pacote que já conhecemos!


# O primeiro passo é conectar com a base de interesse com o comando

# Agora, posso realizar as operações que quiser com o objeto
# Contudo, ele está remoto e não no meu computador, por isso é um pouco limitado

nome_base <- "br_ana_atlas_esgotos.municipio"

base_remota <- basedosdados::bdplyr(nome_base)

base_remota
glimpse(base_remota)

# nosso objetivo é verificar a população atendida por esgoto nos municipios

base_remota_preparada <- base_remota %>% 
  mutate(
    prop_atendimento = populacao_atendida_2035 / populacao_urbana_2035
  ) %>% 
  select(id_municipio, sigla_uf, prop_atendimento) 


# base auxiliar de municipios

base_auxiliar <- bdplyr("br_bd_diretorios_brasil.municipio")

base_auxiliar <- base_auxiliar %>% 
  select(
    id_municipio, nome, regiao
  )

# juntar as duas bases

base_junta <- base_remota_preparada %>% 
  left_join(base_auxiliar, by = "id_municipio")

# coletar os resultados

base_esgoto_com_municipios <- bd_collect(base_junta)

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
    valor_total = sum(valor_beneficio),
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
  ) %>% 
  bd_collect(tabela_auxilio)

tabela_auxilio_coletada_muni <- tabela_auxilio_muni %>% 
  mutate(
    valor_medio = valor_total / qnt_beneficiarios
  ) %>% 
  arrange(-valor_medio) 

tabela_auxilio_coletada_muni %>% 
  arrange(valor_medio)

# Exercício 1 -------------------------------------------------------------

## A BD possui tabelas de diretórios, muito úteis, para facilitar a integração
# entre outras tabelas. Isso permitirá fazer cruzamentos mútliplos entre bases
# bem diferentes e aparentemente não relacionadas: por exemplo, dados de votação e
# segurança pública.

# Vá ao site da BD e localize e tabela contendo o diretório de municípios.
# Anote abaixo o nome:

print("O diretório de municípios da BD se chama: xxxxxxxxxxx")

# 1) Carregue os pacotes e ative seu billing-id.

# 2) Conecte remotamente com a base e salve em um objeto.

# 3) Salve em disco essa tabela, no formato de sua preferência.

# 4) Explore um pouco essa tabela. O que você entende que são as colunas?

# 5) Qual é o id de seu município?

# 6) Qual é a região de saúde do município de Ipira - SC?

# 7) Selecione apenas o nome dos municípios e a sigla da UF. Qual é o 3º
# estado com mais municípios? E qual o 2º que tem menos?

# Exercício 2 ---------------------------------------------------------------

## Escolha uma tabela disponível no BD+ (https://basedosdados.org/dataset, 
# certifique-se que o filtro por "Tabelas tratadas" está ativo).

# Anote abaixo as strings que levam a ela para facilitar
# <projeto>.<id_conjunto>.<id_tabela>

# Ex: basedosdados.br_mc_indicadores.transferencias_municipio 


## Carregue os pacotes pertinentes

## Faça a importação da tabela que você escolheu no modo remoto (bdplyr)

## Explore inicialmente os seus dados. Quais as colunas? Quais as classes?

## Faça a seleção de algumas colunas

## Faça algum filtro

## É necessário/conveniente modificar a classe de alguma coluna? Se for, faça isso.

## Com summarise, faça alguma análise como média, valor mínimo/máximo, contagem,
# etc



