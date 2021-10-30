# nome da tabela
nome_tabela_auxilio <- "br_mc_auxilio_emergencial.microdados"

# fazer a conexão remota
base_auxilio <- bdplyr(nome_tabela_auxilio)

# ver a cara dela
base_auxilio

#### há servidores públicos recebendo??

# nomes das tabelas
nome_tabela_servidores_civis <- "br_cgu_servidores_executivo_federal.servidores_civis_cadastro"
nome_tabela_servidores_militares <- "basedosdados.br_cgu_servidores_executivo_federal.servidores_militares_cadastro"

# fazer as conexões
base_servidores_civis <- bdplyr(nome_tabela_servidores_civis)

base_auxilio_resumida <- base_auxilio %>% 
  group_by(cpf_beneficiario) %>% 
  summarise(
    valor_recebido = sum(valor_beneficio)
  )

# fazer join
base_join_servidores_auxilio <- base_servidores_civis %>% 
  filter(ano >= 2020) %>% 
  left_join(base_auxilio_resumida, by = c("cpf" = "cpf_beneficiario")) %>% 
  filter(!is.na(valor_recebido)) %>% 
  bd_collect(show_query = TRUE)

#
beepr::beep()
