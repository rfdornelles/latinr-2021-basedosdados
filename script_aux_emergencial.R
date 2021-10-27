
# definir a tabela que vou operar:
nome_tabela <- "br_mc_auxilio_emergencial.microdados"

# fazer a conexão remota
base_remota <- bdplyr(nome_tabela)

# valor médio do beneficio pago por estado
tabela_auxilio <- base_remota %>% 
  select(mes, sigla_uf, valor_beneficio, enquadramento) %>% 
  group_by(sigla_uf) %>% 
  summarise(
    valor_total = sum(valor_beneficio),
    qnt_beneficiarios = n()
  ) 

# coletar os dados
tabela_auxilio_coletada <- bd_collect(tabela_auxilio)

