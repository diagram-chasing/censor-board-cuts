options(scipen=999)
library(tidyverse)
library(data.table)

clean_metadata <- function(df) {
  setDT(df)
  
  # Calculate duration_mins first
  df[, duration_mins := {
    duration_raw = str_extract(duration, "\\d+\\.\\d+")
    ifelse(!is.na(duration_raw),
           as.numeric(substr(duration_raw, 1, regexpr("\\.", duration_raw)-1)) +
             as.numeric(substr(duration_raw, regexpr("\\.", duration_raw)+1, nchar(duration_raw)))/60,
           NA_real_)
  }]
  
  # Then use it in subsequent operations
  df[, `:=`(
    id = format(as.numeric(id), scientific = FALSE),
    category = as.factor(category),
    language = as.factor(language),
    format = as.factor(format),
    applicant = ifelse(applicant == "", NA_character_, applicant),
    certifier = ifelse(certifier == "", NA_character_, certifier),
    has_valid_duration = !is.na(duration_mins),
    has_language = !is.na(language),
    has_synopsis = !is.na(synopsis) & synopsis != ""
  )]
  
  return(df)
}

clean_metadata <- function(df) {
  setDT(df)
  
  # Calculate duration_mins first
  df[, duration_mins := {
    duration_raw = str_extract(duration, "\\d+\\.\\d+")
    ifelse(!is.na(duration_raw),
           as.numeric(substr(duration_raw, 1, regexpr("\\.", duration_raw)-1)) +
             as.numeric(substr(duration_raw, regexpr("\\.", duration_raw)+1, nchar(duration_raw)))/60,
           NA_real_)
  }]
  
  # Then use it in subsequent operations
  df[, `:=`(
    id = format(as.numeric(id), scientific = FALSE),
    category = as.factor(category),
    language = as.factor(language),
    format = as.factor(format),
    applicant = ifelse(applicant == "", NA_character_, applicant),
    certifier = ifelse(certifier == "", NA_character_, certifier),
    has_valid_duration = !is.na(duration_mins),
    has_language = !is.na(language),
    has_synopsis = !is.na(synopsis) & synopsis != ""
  )]
  
  return(df)
}
clean_modifications <- function(df) {
  # Pre-compile all patterns
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
  
  type_patterns <- list(
    song = "song|music|lyric",
    dialogue = "dialogue|word|line|speak|utter",
    scene = "scene|visual|shot",
    title = "title|credit|card",
    technical = "tcr|time|duration"
  )
  
  time_patterns <- paste0(c(
    "(?i)TCR-?:?\\s*\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?",
    "(?<!\\d)\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?(?!\\d)",
    "\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?\\s*(?:to|-|TO)\\s*\\d{2}[:.']\\d{2}[:.']\\d{2}(?:[:.']\\d{2})?",
    "\\d{1,2}\\.\\d{2}(?:\\s*(?:to|-|TO)\\s*\\d{1,2}\\.\\d{2})?",
    "\\d{1,2}:\\d{2}\\s*(?:to|-|TO)\\s*\\d{1,2}:\\d{2}",
    "\\d+\\s*(?:hour|hr)s?\\s+\\d+\\s*(?:minute|min)s?\\s+(?:\\d+\\s*sec)?s?",
    "\\d+\\s*mins?\\s+(?:\\d+\\s*sec)?s?",
    "\\d+\\s*Sec(?:s|\\.)?",
    "(?<!\\d)\\d+\\.\\d{2}(?!\\d)",
    "(?:(?<!\\d)\\d{1,2}\\.\\d{2}(?!\\d)(?:,\\s*)?)+",
    "\\d+\\.\\d{2}\\s*mins?"
  ), collapse = "|")
  
  setDT(df)
  
  df[, `:=`(
    certificate_id = format(as.numeric(certificate_id), scientific = FALSE),
    description = str_trim(description),
    cut_no = as.integer(cut_no),
    
    tcr_timestamps = {
      matches <- str_extract_all(description[!is.na(description)], time_patterns)
      cleaned_times <- lapply(matches, function(times) {
        if(length(times) == 0) return("NANA")
        times <- str_replace_all(times, "(?i)TCR-?:?\\s*", "")
        times <- str_replace_all(times, "(?i)\\s*(?:hour|hr)s?\\s+", ":")
        times <- str_replace_all(times, "(?i)\\s*(?:minute|min)s?\\s+", ":")
        times <- str_replace_all(times, "(?i)\\s*secs?\\s*", "")
        paste(unique(times), collapse = ", ")
      })
      unlist(cleaned_times)
    },
    
    deleted = fcase(
      is.na(deleted) | deleted == "", 0,
      str_detect(as.character(deleted), "\\."), {
        parts = str_split_fixed(as.character(deleted), "\\.", 2)
        as.numeric(parts[,1]) + as.numeric(parts[,2])/60
      },
      default = as.numeric(deleted)
    ),
    
    replaced = fcase(
      is.na(replaced) | replaced == "", 0,
      str_detect(as.character(replaced), "\\."), {
        parts = str_split_fixed(as.character(replaced), "\\.", 2)
        as.numeric(parts[,1]) + as.numeric(parts[,2])/60
      },
      default = as.numeric(replaced)
    ),
    
    inserted = fcase(
      is.na(inserted) | inserted == "", 0,
      str_detect(as.character(inserted), "\\."), {
        parts = str_split_fixed(as.character(inserted), "\\.", 2)
        as.numeric(parts[,1]) + as.numeric(parts[,2])/60
      },
      default = as.numeric(inserted)
    )
  )]
  
  df[, `:=`(
    mod_type = fcase(
      str_detect(tolower(description), mod_patterns$audio), "audio",
      str_detect(tolower(description), mod_patterns$visual), "visual",
      str_detect(tolower(description), mod_patterns$deletion), "deletion",
      str_detect(tolower(description), mod_patterns$insertion), "insertion",
      str_detect(tolower(description), mod_patterns$overlay), "overlay",
      str_detect(tolower(description), mod_patterns$reduction), "reduction",
      str_detect(tolower(description), mod_patterns$replacement), "replacement",
      str_detect(tolower(description), mod_patterns$translation), "translation",
      str_detect(tolower(description), mod_patterns$spacing), "spacing",
      str_detect(tolower(description), mod_patterns$disclaimer), "disclaimer",
      default = "other"
    ),
    
    content_category = fcase(
      str_detect(tolower(description), content_patterns$violence), "violence",
      str_detect(tolower(description), content_patterns$sexual), "sexual", 
      str_detect(tolower(description), content_patterns$substance), "substance",
      str_detect(tolower(description), content_patterns$profanity), "profanity",
      str_detect(tolower(description), content_patterns$religious), "religious",
      str_detect(tolower(description), content_patterns$gestures), "gestures",
      str_detect(tolower(description), content_patterns$social), "social",
      str_detect(tolower(description), content_patterns$political), "political",
      default = "other"
    ),
    
    content_type = fcase(
      str_detect(tolower(description), type_patterns$song), "song",
      str_detect(tolower(description), type_patterns$dialogue), "dialogue",
      str_detect(tolower(description), type_patterns$scene), "scene",
      str_detect(tolower(description), type_patterns$title), "title",
      str_detect(tolower(description), type_patterns$technical), "technical",
      default = "other"
    ),
    
    total_modified_time = round(deleted + replaced + inserted, 2)
  )]
  
  setcolorder(df, c(
    "certificate_id", "film_name", "cut_no",
    "mod_type", "content_category", "content_type", "description", "tcr_timestamps",
    "deleted", "replaced", "inserted", "total_modified_time"
  ))
  
  return(df)
}

# Usage
modifications <- fread('../../data/raw/modifications.csv')
modifications <- clean_modifications(modifications)
metadata <- fread('../../data/raw/metadata.csv')
metadata <- clean_metadata(metadata)
