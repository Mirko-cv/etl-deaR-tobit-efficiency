library(dplyr)
library(readxl)

ruta <- "D:\\GITHUB\\DEA\\todas_las_tablas.xlsx"

df<-purrr::map_dfr(excel_sheets(ruta),
               ~read_excel(ruta, sheet = .x, range = "A1:G18",col_names=F) %>%
                 mutate(sheet = .x)
               )

df <- as.data.frame(
  lapply(df, function(x) {
    if (is.character(x)) {
      gsub("[\r\n]+", " ", x)
    } else {
      x
    }
  })
)


###################
# proceso de ETL #
##################

#obtenemos el nombre de la empresa
library(stringr)

df1 <- df %>% # quitamos espacios de mas y conservamos el nombre de la EPS
  mutate(`...1` = str_replace_all(`...1`, "(.)\\1+", "\\1")) %>% 
  mutate(`...1` = toupper(`...1`)) %>% 
  mutate(`...1` = case_when(
    grepl("^DIRE", `...1`, ignore.case = TRUE) ~ 
      str_extract(
        str_to_upper(`...1`),
        "(EPS\\s+(?:\\p{L}+\\s*)+(?:S\\.A\\.)?|SED\\p{L}+(?:\\s*S\\.A\\.)?)"
      ),
    TRUE ~ `...1`
  )) %>% # repetimos el nombre de la EPS
  mutate(EPS=NA) %>% 
  mutate(
    across(
      everything(),
      ~ if_else(
        str_detect(`...1`, "^(EPS|SEDA)"),
        as.character(`...1`),
        .
      )
    )
  ) %>% 
  tidyr::fill(EPS, .direction = "down") # rellenamos hacia abajo (con el nombre de la ep)
# con esto tendremos un dataframe fortmato largo
  
#validacion 
#df1 %>% select(EPS,1,2) %>% filter(grepl("^(RE|TIPO|RAT|N°|POBLAC|GRU|NRO)",`...1` ) ) %>% select(EPS) %>% table() %>% sort()
 

 
df_v1<-df1 %>% select(EPS,1,2) %>%   
  filter(grepl("^(RE|TIPO|RAT|N°|POBLAC|GRU|NRO)",`...1` ) ) %>% 
  mutate(
    ...1 = ...1[((row_number() - 1) %% 10) + 1]
  ) %>%
  tidyr::pivot_wider(id_cols = EPS,names_from =`...1`,values_from = `...2` ) %>%
  mutate(across(c(-1,-2,-3,-4,-7), ~ as.numeric(gsub(",", "", as.character(.))))) %>% 
  mutate(across(c(-1,-2,-3,-4,-7), ~ as.numeric(as.character(.))))

df_v2<- df1 %>% select(EPS,5,7) %>%   
  filter(grepl("^(Cober|Continuidad|Densidad|Relac|Microme|Indica|Cumplimi|Promedio)",`...5` ) ) %>% 
  mutate(
    ...5 = ...5[((row_number() - 1) %% 10) + 1]
  )%>%
  tidyr::pivot_wider(id_cols = EPS,names_from =`...5`,values_from = `...7` ) %>% 
  mutate(across(-1, ~ as.numeric(as.character(.))))

## join

df_final<-df_v1 %>% inner_join(df_v2,by="EPS")

openxlsx::write.xlsx(df_final,"D:\\GITHUB\\DEA\\data_EPS_final.xlsx")

