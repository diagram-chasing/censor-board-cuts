library(tidyverse)
metadata <- read_csv("../../data/raw/metadata.csv")
modifications <- read_csv('../../data/raw/modifications.csv')

modifications %>% sample_n(10) %>% as_tibble() %>% clipr::write_clip()
clean_metadata <- function(df) {
  df %>%
    mutate(id = format(as.numeric(id), scientific = FALSE)) %>%
    

    mutate(
      duration = str_extract(duration, "\\d+\\.\\d+"),
      duration_mins = case_when(
        !is.na(duration) ~ as.numeric(str_split_fixed(duration, "\\.", 2)[,1]) +
          as.numeric(str_split_fixed(duration, "\\.", 2)[,2])/60,
        TRUE ~ NA_real_
      )
    ) %>%
    
    mutate(
      category = factor(category),
      language = factor(language),
      format = factor(format)
    ) %>%
    
    mutate(
      applicant = na_if(applicant, ""),
      certifier = na_if(certifier, ""),
    ) %>%
    
    mutate(
      has_valid_duration = !is.na(duration_mins),
      has_language = !is.na(language),
      has_synopsis = !is.na(synopsis) & synopsis != ""
    )
}

metadata <- clean_metadata(metadata)
