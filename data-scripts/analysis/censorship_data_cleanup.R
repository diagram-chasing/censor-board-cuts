library(tidyverse)


# modifications %>% sample_n(10) %>% as_tibble() %>% clipr::write_clip()
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



clean_modifications <- function(df) {
  # Modification type patterns
  mod_patterns <- list(
    audio = "muted|mute|sound|voice|audio|sync",
    visual = "blur|defocus|black|white",
    deletion = "delet|remov|cut|trim",
    insertion = "insert|add|includ",
    overlay = "superimpos|overlay",
    reduction = "reduc|decreas|50|percent",
    replacement = "replac|modif|chang|correct",
    translation = "translat|subtitle|language", 
    spacing = "space|blank|slot",
    disclaimer = "warning|statutory|disclaimer|certificate"
  )
  
  # Content category patterns
  content_patterns <- list(
    violence = "blood|kill|stab|shoot|fight|wound|dead|murder|gore|brutal|slit|chop|bullet|gun",
    sexual = "rape|intimate|kiss|bed|romance|nude|naked|sex|breast|cleavage|vulgar|obscene|adult scene",
    substance = "smoke|drug|alcohol|liquor|drinking|ganja|weed|narcotic|tobacco",
    profanity = "fuck|bitch|ass|dick|bastard|slut|muth|gaand|pimp|whore|word",
    religious = "hindu|muslim|temple|mosque|church|god|allah|christ|pray|worship",
    gestures = "middle finger|gesture|sign|symbol",
    social = "caste|religion|community|race|ethnic|dowry|class",
    political = "modi|gandhi|minister|party|election|vote|government"
  )
  
  # Content type patterns  
  type_patterns <- list(
    song = "song|music|lyric",
    dialogue = "dialogue|word|line|speak|utter",
    scene = "scene|visual|shot",
    title = "title|credit|card",
    technical = "tcr|time|duration"
  )
  
  df %>%
    mutate(certificate_id = format(as.numeric(certificate_id), scientific = FALSE)) %>%
    mutate(
      across(where(is.character), str_trim),
      cut_no = as.integer(cut_no),
      
      tcr_timestamps = map_chr(description, function(desc) {
        if(is.na(desc)) return("NANA")
        
        # Extract all time patterns
        patterns <- c(
          # TCR format
          "(?i)TCR-?:?\\s*\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?",
          # Standard time format
          "(?<!\\d)\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?(?!\\d)",
          # Time ranges
          "\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?\\s*(?:to|-|TO)\\s*\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?",
          # Decimal format
          "\\d{1,2}\\.\\d{2}(?:\\s*(?:to|-|TO)\\s*\\d{1,2}\\.\\d{2})?",
          # Short format with colon
          "\\d{1,2}:\\d{2}\\s*(?:to|-|TO)\\s*\\d{1,2}:\\d{2}",
          # Text format with hours and minutes
          "\\d+\\s*(?:hour|hr)s?\\s+\\d+\\s*(?:minute|min)s?\\s+(?:\\d+\\s*sec)?s?",
          # Minutes and seconds format
          "\\d+\\s*mins?\\s+(?:\\d+\\s*sec)?s?",
          # Just seconds
          "\\d+\\s*Sec(?:s|\\.)?",
          # Minutes format
          "(?<!\\d)\\d+\\.\\d{2}(?!\\d)",
          # Time lists
          "(?:(?<!\\d)\\d{1,2}\\.\\d{2}(?!\\d)(?:,\\s*)?)+",
          # Time with mins suffix
          "\\d+\\.\\d{2}\\s*mins?"
        )
        
        all_times <- character(0)
        for(pattern in patterns) {
          matches <- str_extract_all(desc, pattern)[[1]]
          all_times <- c(all_times, matches)
        }
        
        if(length(all_times) == 0) return("NANA")
        
        # Standardize formats
        all_times <- str_replace_all(all_times, "(?i)TCR-?:?\\s*", "")
        all_times <- str_replace_all(all_times, "(?i)\\s*(?:hour|hr)s?\\s+", ":")
        all_times <- str_replace_all(all_times, "(?i)\\s*(?:minute|min)s?\\s+", ":")
        all_times <- str_replace_all(all_times, "(?i)\\s*secs?\\s*", "")
        
        paste(unique(all_times), collapse = ", ")
      }),
      
      description = str_trim(description),
      
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
        str_detect(tolower(description), mod_patterns$audio) ~ "audio",
        str_detect(tolower(description), mod_patterns$visual) ~ "visual",
        str_detect(tolower(description), mod_patterns$deletion) ~ "deletion",
        str_detect(tolower(description), mod_patterns$insertion) ~ "insertion", 
        str_detect(tolower(description), mod_patterns$overlay) ~ "overlay",
        str_detect(tolower(description), mod_patterns$reduction) ~ "reduction",
        str_detect(tolower(description), mod_patterns$replacement) ~ "replacement",
        str_detect(tolower(description), mod_patterns$translation) ~ "translation",
        str_detect(tolower(description), mod_patterns$spacing) ~ "spacing",
        str_detect(tolower(description), mod_patterns$disclaimer) ~ "disclaimer",
        TRUE ~ "other"
      ),
      
      content_category = case_when(
        str_detect(tolower(description), content_patterns$violence) ~ "violence",
        str_detect(tolower(description), content_patterns$sexual) ~ "sexual",
        str_detect(tolower(description), content_patterns$substance) ~ "substance",
        str_detect(tolower(description), content_patterns$profanity) ~ "profanity",
        str_detect(tolower(description), content_patterns$religious) ~ "religious",
        str_detect(tolower(description), content_patterns$gestures) ~ "gestures",
        str_detect(tolower(description), content_patterns$social) ~ "social",
        str_detect(tolower(description), content_patterns$political) ~ "political",
        TRUE ~ "other"
      ),
      
      content_type = case_when(
        str_detect(tolower(description), type_patterns$song) ~ "song",
        str_detect(tolower(description), type_patterns$dialogue) ~ "dialogue",
        str_detect(tolower(description), type_patterns$scene) ~ "scene", 
        str_detect(tolower(description), type_patterns$title) ~ "title",
        str_detect(tolower(description), type_patterns$technical) ~ "technical",
        TRUE ~ "other"
      ),
      
      total_modified_time = round(deleted + replaced + inserted, 2)
    ) %>%
    select(
      certificate_id, film_name, cut_no,
      mod_type, content_category, content_type, description, tcr_timestamps,
      deleted, replaced, inserted, total_modified_time,
      everything()
    )
}
# metadata <- read_csv("../../data/raw/metadata.csv")
modifications <- read_csv('../../data/raw/modifications.csv')
modifications <- clean_modifications(modifications)
metadata <- clean_metadata(metadata)
