## Rodrigo Dornelles - Sun Jun 27 13:23:28 2021
##
## Objetivo: baixar bases de dados úteis para praticar
# clusterização

# Pacotes -----------------------------------------------------------

library(magrittr)
library(tibble)


# Set base dos dados ------------------------------------------------------

basedosdados::set_billing_id("rfdornelles-bq")
bigrquery::bq_auth(email = "rodornelles@gmail.com")

# população brasileira por municipio
# ano 2020

base_pop <- basedosdados::bdplyr("br_ibge_populacao.municipio")

base_pop <- base_pop %>%
  dplyr::filter(ano == 2020)

# join com nome e outros dados
base_municipios <- basedosdados::bdplyr("br_bd_diretorios_brasil.municipio")

base_municipios <- base_municipios %>%
  dplyr::select(id_municipio, id_municipio_6,
                nome, capital_uf,
                sigla_uf, regiao)

base_municipios_pop <- base_municipios %>%
  dplyr::left_join(base_pop) %>%
  basedosdados::bd_collect()

# covid
# SELECT location_key, date, country_name, aggregation_level, cumulative_confirmed, cumulative_deceased, cumulative_persons_vaccinated, cumulative_persons_fully_vaccinated, population, human_development_index, area_sq_km, international_travel_controls, restrictions_on_internal_movement, fiscal_measures, public_information_campaigns, contact_tracing, average_temperature_celsius, life_expectancy
# https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/README.md

base_covid_mundo <- basedosdados::bdplyr("bigquery-public-data.covid19_open_data.covid19_open_data")

base_covid_mundo <- base_covid_mundo %>%
  dplyr::select(location_key, date, country_name,
                aggregation_level, subregion1_name,
                cumulative_confirmed, cumulative_deceased,
                cumulative_persons_vaccinated,
                cumulative_persons_fully_vaccinated,
                cumulative_tested_female,
                cumulative_tested_male,
                population,
                human_development_index, area_sq_km,
                international_travel_controls,
                restrictions_on_internal_movement,
                public_information_campaigns,
                contact_tracing,
                average_temperature_celsius, life_expectancy,
                gdp_per_capita_usd, facial_coverings,
                location_geometry) %>%
  dplyr::filter(aggregation_level == 0, !is.na(cumulative_deceased),
                !is.na(cumulative_confirmed)) %>%
  basedosdados::bd_collect() %>%
  dplyr::ungroup()

# ajustar as categóricas
base_covid_mundo <- base_covid_mundo %>%
  dplyr::mutate(
    international_travel_controls = factor(
      x = international_travel_controls,
      levels = 0:4,
      labels = c(
        "sem restrição",
        "triagem de chegadas",
        "quarentena de chegada",
        "banimento de chedasas",
        "fechamento total")
    ),
    restrictions_on_internal_movement = factor(
      x = restrictions_on_internal_movement,
      levels = 0:2,
      labels = c(
        "sem medidas",
        "recomendações de não viajar",
        "restrições de mobilidade"
      )),
    public_information_campaigns = factor(
      x = public_information_campaigns,
      levels = 0:2,
      labels = c(
        "sem campanhas",
        "discursos oficiais de alerta",
        "campanha coordenada"
      )
    ),
    contact_tracing = factor(
      x = contact_tracing,
      levels = 0:2,
      labels = c(
        "nenhum rastreio",
        "rastreio limitado",
        "rastreio completo"
      )
    ),
    facial_coverings = factor(
      x = facial_coverings,
      levels = 0:4,
      labels = c(
        "sem política",
        "recomendado",
        "obrigatório em locais específicos",
        "obrigatório em todos os locais públicos com pessoas",
        "obrigatório fora de casa sempre"
      )
    )
  )



# covid BR

base_covid_br <- basedosdados::bdplyr("bigquery-public-data.covid19_open_data.covid19_open_data")

base_covid_br <- base_covid_br %>%
  dplyr::select(date, country_name,
                aggregation_level,
                subregion1_code, subregion1_name,
                subregion2_code, subregion2_name,
                cumulative_confirmed, cumulative_deceased,
                cumulative_persons_vaccinated,
                cumulative_persons_fully_vaccinated,
                population, human_development_index, area_sq_km,
                international_travel_controls,
                restrictions_on_internal_movement,
                fiscal_measures,
                public_information_campaigns,
                contact_tracing,
                average_temperature_celsius, life_expectancy) %>%
  dplyr::filter(aggregation_level == 2,
                country_name == "Brazil",
                !is.na(cumulative_deceased),
                !is.na(cumulative_confirmed))  %>%
  dplyr::group_by(subregion1_name) %>%
  dplyr::filter(date == max(date)) %>%
  basedosdados::bd_collect()

# SSP
base_ssp <- basedosdados::bdplyr("br_sp_gov_ssp.ocorrencias_registradas")

base_ssp <- base_ssp %>%
  dplyr::filter(ano >= 2020) %>%
  dplyr::mutate(id_municipio = as.character(id_municipio)) %>%
  basedosdados::bd_collect()


# base_oxford -------------------------------------------------------------

base_oxford <- readr::read_csv("https://github.com/OxCGRT/covid-policy-tracker/raw/master/data/OxCGRT_latest.csv",
                               guess_max = 100000)


# Salvar tudo -------------------------------------------------------------

usethis::use_data(base_covid_br, overwrite = TRUE, version = 3)
usethis::use_data(base_covid_br, overwrite = TRUE, version = 3)
usethis::use_data(base_covid_mundo, overwrite = TRUE, version = 3)
usethis::use_data(base_municipios_pop, overwrite = TRUE, version = 3)
usethis::use_data(base_ssp, overwrite = TRUE, version = 3)
usethis::use_data(base_oxford, overwrite = TRUE, version = 3)
