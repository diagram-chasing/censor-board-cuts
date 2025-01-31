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
clean_modifications <- function(df) {
  df %>%
    mutate(
      across(where(is.character), str_trim),
      cut_no = as.integer(cut_no),
      
      tcr_timestamps = map_chr(description, function(desc) {
        if(is.na(desc)) return("NANA")
        
        # Extract all time patterns 
        all_times = c(
          str_extract_all(desc, "(?i)TCR-?\\s*\\d{2}:\\d{2}:\\d{2}(?::\\d{2})?")[[1]],
          str_extract_all(desc, "(?<!\\d)\\d{2}:\\d{2}:\\d{2}(?::\\d{2})?(?!\\d)")[[1]],
          str_extract_all(desc, "\\d{2}:\\d{2}:\\d{2}\\s*-\\s*\\d{2}:\\d{2}:\\d{2}")[[1]]
        )
        
        if(length(all_times) == 0) return("NANA")
        paste(all_times, collapse = ", ")
      }),
      
      description = str_replace_all(description, 
                                    "(?i)(?:TCR-?\\s*)?\\d{2}:\\d{2}:\\d{2}(?::\\d{2})?(?:\\s*(?:to|-)\\s*\\d{2}:\\d{2}:\\d{2}(?::\\d{2})?)?", 
                                    ""
      ),
      description = str_replace_all(description, "[,;]\\s*and\\s*", ", "),
      description = str_replace_all(description, "\\s+", " "),
      description = str_trim(description),
      
      # Format numeric columns with 2 decimal places
      deleted = round(case_when(
        is.na(deleted) | deleted == "" ~ 0,
        str_detect(as.character(deleted), "\\.") ~ {
          parts = str_split_fixed(as.character(deleted), "\\.", 2)
          as.numeric(parts[,1]) + as.numeric(parts[,2])/60
        },
        TRUE ~ as.numeric(deleted)
      ), 2),
      
      replaced = round(case_when(
        is.na(replaced) | replaced == "" ~ 0,
        str_detect(as.character(replaced), "\\.") ~ {
          parts = str_split_fixed(as.character(replaced), "\\.", 2)
          as.numeric(parts[,1]) + as.numeric(parts[,2])/60
        },
        TRUE ~ as.numeric(replaced)
      ), 2),
      
      inserted = round(case_when(
        is.na(inserted) | inserted == "" ~ 0,
        str_detect(as.character(inserted), "\\.") ~ {
          parts = str_split_fixed(as.character(inserted), "\\.", 2)
          as.numeric(parts[,1]) + as.numeric(parts[,2])/60
        },
        TRUE ~ as.numeric(inserted)
      ), 2),
      
      mod_type = case_when(
        str_detect(tolower(description), "muted|mute") ~ "audio_mute",
        str_detect(tolower(description), "delet|remov") ~ "deletion",
        str_detect(tolower(description), "blur") ~ "visual_blur",
        str_detect(tolower(description), "insert|add") ~ "insertion",
        str_detect(tolower(description), "superimpos") ~ "overlay", 
        str_detect(tolower(description), "reduc") ~ "reduction",
        str_detect(tolower(description), "replac") ~ "replacement",
        str_detect(tolower(description), "modif") ~ "modification",
        str_detect(tolower(description), "space|blank") ~ "spacing",
        TRUE ~ "other"
      ),
      
      total_modified_time = round(deleted + replaced + inserted, 2)
    ) %>%
    select(
      certificate_id, film_name, cut_no,
      mod_type, description, tcr_timestamps,
      deleted, replaced, inserted, total_modified_time,
      everything()
    )
}
modifications <- clean_modifications(modifications)
