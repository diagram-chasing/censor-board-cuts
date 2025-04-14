# install.packages(c("tidyverse", "data.table", "arrow", "lubridate", "purrr", "stringr"))
library(tidyverse)
library(data.table)
library(arrow)
library(lubridate)
library(stringr)
library(purrr)

options(scipen=999)
options(datatable.allow.cartesian = TRUE) 
raw_modifications_path <- '../../data/raw/modifications.csv'
raw_metadata_path <- '../../data/raw/metadata.csv'
output_cleaned_path <- "../../data/censorship_data_cleaned.parquet"

#' Cleans basic metadata columns. Standardizes ID to character, trimmed, leading zeros removed.
#'
#' @param df Dataframe containing metadata. Assumes data.table format.
#' @return Dataframe with cleaned metadata columns.
clean_metadata <- function(df) {
  setDT(df) # Convert to data.table by reference
  
  if ("id" %in% names(df)) {
    # Ensure character, trim whitespace, remove leading zeros (preserving "0")
    if(!is.character(df$id)) df[, id := as.character(id)]
    df[, id := str_trim(id)]
    # Remove leading zeros carefully: sub("^0+", "", "0") yields "", so handle "0" explicitly
    df[, id := fifelse(id == "0", "0", sub("^0+", "", id))]
    # Handle potential NAs introduced if original was NA
    df[is.na(id), id := NA_character_]
  } else {
    warning("Column 'id' not found in metadata.")
    # Consider adding an empty NA column if needed downstream, but join will fail anyway
  }
  
  # --- Clean and calculate duration_mins ---
  if ("duration" %in% names(df)) {
    df[, duration_mins := {
      duration_chr = as.character(duration) # Work with character vector
      duration_raw = str_extract(duration_chr, "\\d+\\.\\d+")
      duration_numeric = suppressWarnings(as.numeric(duration_raw))
      
      iso_duration = str_match(duration_chr, "PT(?:(\\d+)H)?(?:(\\d+)M)?(?:(\\d+(?:\\.\\d+)?)S)?")
      iso_mins = numeric(length(duration_chr))
      for(i in seq_len(nrow(iso_duration))) {
        if(!is.na(iso_duration[i,1])) {
          hrs = ifelse(is.na(iso_duration[i,2]), 0, suppressWarnings(as.numeric(iso_duration[i,2])))
          mins = ifelse(is.na(iso_duration[i,3]), 0, suppressWarnings(as.numeric(iso_duration[i,3])))
          secs = ifelse(is.na(iso_duration[i,4]), 0, suppressWarnings(as.numeric(iso_duration[i,4])))
          # Handle potential NAs from conversion
          hrs = ifelse(is.na(hrs), 0, hrs)
          mins = ifelse(is.na(mins), 0, mins)
          secs = ifelse(is.na(secs), 0, secs)
          iso_mins[i] = hrs * 60 + mins + secs / 60
        } else {
          iso_mins[i] = NA_real_
        }
      }
      
      calculated_mins = fifelse(!is.na(iso_mins) & iso_mins > 0, iso_mins,
                                fifelse(!is.na(duration_numeric), duration_numeric, NA_real_))
      calculated_mins
    }]
  } else {
    df[, duration_mins := NA_real_]
  }
  
  # Convert to factor only if column exists and is not already factor
  if ("category" %in% names(df) && !is.factor(df$category)) df[, category := as.factor(category)]
  if ("language" %in% names(df) && !is.factor(df$language)) df[, language := as.factor(language)]
  if ("format" %in% names(df) && !is.factor(df$format)) df[, format := as.factor(format)]
  
  # Handle empty strings as NA for applicant/certifier
  if ("applicant" %in% names(df)) df[applicant == "", applicant := NA_character_]
  if ("certifier" %in% names(df)) df[certifier == "", certifier := NA_character_]
  
  # Ensure cert_date is handled correctly if present (used later)
  if ("cert_date" %in% names(df)) {
    if (!inherits(df$cert_date, "Date")) {
      # Attempt parsing, create placeholder if it fails
      df[, cert_date_parsed := suppressWarnings(dmy(cert_date))]
      if(any(is.na(df$cert_date_parsed) & !is.na(df$cert_date) & df$cert_date != "" )) {
        warning("Some non-empty certificate dates in metadata could not be parsed to Date format.")
      }
    } else {
      # If already Date, just copy it
      df[, cert_date_parsed := cert_date]
    }
  } else {
    df[, cert_date_parsed := as.Date(NA)]
  }
  
  
  return(df)
}


#' Cleans modification descriptions, standardizes certificate_id, calculates times.
#'
#' @param df Dataframe containing modification details. Assumes data.table format.
#' @param description_col Name of the column with descriptions (character string).
#' @return Dataframe with cleaned columns and added tags/times.
clean_modifications <- function(df, description_col = "description") {
  
  if (!description_col %in% names(df)) {
    stop(paste("Column '", description_col, "' not found in the dataframe."))
  }
  
  # Ensure input is a data.table
  if (!is.data.table(df)) {
    setDT(df)
  }
  
  # --- Clean certificate_id ---
  if ("certificate_id" %in% names(df)) {
    # Ensure character, trim whitespace, remove leading zeros (preserving "0")
    if(!is.character(df$certificate_id)) df[, certificate_id := as.character(certificate_id)]
    df[, certificate_id := str_trim(certificate_id)]
    # Remove leading zeros carefully
    df[, certificate_id := fifelse(certificate_id == "0", "0", sub("^0+", "", certificate_id))]
    df[is.na(certificate_id), certificate_id := NA_character_]
  } else {
    stop("Critical column 'certificate_id' not found in modifications data.")
  }
  
  mod_patterns <- list(
    audio_mute = "\\b(muted?|mute|silence|beep)|audio.*(remov|delet)|voice.*(remov|delet)|sound.*(remov|delet)",
    audio_level = "\\b(volume|mix|level)|audio.*(adjust|rais|lower|adjusted?)",
    audio_replace = "audio dub|dubbed|voice over|re-?record|audio.*replac(ed?)?",
    audio_effect = "sound effect|audio effect",
    visual_blur = "\\b(blur(red)?|defocus|pixelate|mask(ed)?)",
    visual_censor = "censor(ed)?|cover(ed)?|black(ed)?.*out|white.*out",
    visual_effect = "graphic|animation|visual effect|vfx|cgi",
    visual_adjust = "color correct(ed)?|brightness|contrast|grain|stabiliz(ed)?",
    visual_framerate = "frame rate|speed.*up|slow.*down",
    deletion = "\\b(delete|delet(ed)?|remov(ed)?|cut|trim|omitted?|excised?|erase|expunge)\\b",
    insertion = "\\b(insert(ed)?|add(ed)?|includ(ed)?|append(ed)?)\\b",
    overlay = "overlay|superimpos|logo|watermark|scroll",
    reduction = "\\b(reduc(ed)?|decreas(ed)?|lessen(ed)?|diminish(ed)?|shorten(ed)?|abridge(d)?|condense(d)?|crop(ped)?)\\b",
    replacement = "\\b(replac(ed)?|modif(ied)?|chang(ed)?|correct(ed)?|alternate|substitute|version|edit(ed)?|alternative take|re-?edit)\\b",
    translation = "\\b(translat(ed)?|subtitle(d)?|dub(bed)?|caption(ed)?|\\bCC\\b|subbed|localization|language)\\b",
    spacing = "\\b(space|blank|slot|gap|pause|interval|padding|timing)\\b",
    warning_disclaimer = "\\b(warning|statutory|disclaimer|notice|advisory|legal|health spot|anti-?smoking|anti-?tobacco)\\b",
    certification = "\\b(certificate|certification|cbfc)\\b"
  )
  
  content_patterns <- list(
    violence_physical = "blood|kill|stab|shoot|fight|wound|dead|murder|gore|brutal|slit|chop|bullet|gun|torture|assault|injury|mutilate|strangle|beating|attack|combat|aggression|physical violence|weapon|dangerous action|stunt|crackers",
    violence_destruction = "blast|explosion|destroy|damage",
    sexual_explicit = "\\b(rape|nude|naked|sex(?!ism|ual)|breast|vulgar|obscene|adult scene|erotic|sex scene|intercourse|orgasm|porn|sexual assault|harassment|molest)\\b",
    sexual_suggestive = "intimate|kiss|bed|romance|cleavage|foreplay|suggestive|teasing|flirting|sensual|lip ?lock|making out|navel|buttock|nipple|thigh",
    substance_use = "\\b(smoke|drug|alcohol|liquor|drinking|ganja|addiction|substance abuse|overdose|weed|narcotic|tobacco|intoxication|dope|smuggling|cigarette|bidi|cigar|vape|inject|cocaine|heroin|opium|whisky|beer|vodka|rum)\\b",
    substance_brand = "\\b(brand|label|kingfisher|budweiser|old monk|black dog|vat 69|heineken|bacardi|red bull|panama|wills|gold flake|scissors|hans|shambu|rajnigandha)\\b",
    profanity = "\\b(fuck|bitch|ass|dick|bastard|slut|muth|gaand|pimp|middle finger|whore|offensive gesture|hand gesture|damn|hell|crap|shit|curse|cuss|expletive|offensive language|slur|maire|polayadi mole|nayinte mon|pottan|ko?lladi|saala|chutiya|madarchod|behenchod|haramzada|haramkhor|kutta|kamina|randi|maal|gandu|bosudi|chootiya|thevidiya|punda|otha|mayir|kundi|kazhuveri|poda|podi)\\b",
    religious = "\\b(hindu|muslim|christian|sikh|buddhist|jewish|temple|mosque|church|gurudwara|synagogue|god|allah|christ|bhagwan|prophet|pray|worship|blasphemy|sacrilege|sectarian|faith|deity|religious figure|religious text|guru|swami|sree narayana guru|bible|quran|gita|vedas|jesus|yesu|ram|krishna|shiva|ganesh|durga|kali|mother teresa|pope|brahmin|iyer|iyengar)\\b",
    social_commentary = "\\b(caste|religion|community|race|ethnic|dowry|class|discrimination|prejudice|bias|stereotype|gender|sexism|social issue|social commentary|poverty|inequality|lgbt|gay|lesbian|transgender|hijra|dalit|harijan)\\b",
    political = "\\b(modi|gandhi|minister|party|election|vote|government|sarkar|sarkaar|neta|politician|protest|ideology|propaganda|nationalism|activism|rally|sedition|political figure|national anthem|flag|congress|bjp|rss|evm|parliament|rahul|sonia|kejriwal|advani|vajpayee|nehru|indira|cpm|trs|janasena|tdp|dmk|admk|kcr|cbn|jagan)\\b|pawan kalyan|stalin|jayalalitha|karunanidhi",
    group_reference = "\\b(american|pakistani|chinese|tamil|malayalam|telugu|kannada|hindi|bengali|punjabi|gujarati|marathi|odia|assamese|nepali|russian|british|sri lankan|bangladeshi)\\b"
  )
  
  type_patterns <- list(
    song_music = "song|music|lyric|soundtrack|background music|score|musical number|bgm",
    dialogue_speech = "dialogue|word|line|speak|utter|monologue|conversation|speech|remark|comment|exchange|voice|audio|sound|thanathum asthanathum",
    scene_visual = "scene|visual|shot|sequence|act|setting|backdrop|cutaway|establishing shot|footage|frame|image|picture|head",
    text_title = "title|credit|card|opening credits|ending credits|subtitle|text card|caption|scroll|super|text|font",
    brand_logo = "brand|label|logo|symbol",
    technical_meta = "\\b(tcr|time|duration|frame|aspect ratio|resolution|encoding|format|technical issue|glitch|artifact|sync)\\b",
    certificate_disclaimer = "certificate|disclaimer|warning|health spot|notice|statutory"
  )
  
  # Timestamp Regex for REMOVAL from description
  time_regex_for_removal <- paste0(
    "(?i)",
    # Specific TCR formats (more robust)
    "\\b(?:TCR-?\\s*)?\\d{2}[:.]\\d{2}[:.]\\d{2}[:.]\\d{2}\\b",
    # General time formats H:M:S, M:S
    "|\\b\\d{1,2}[:.]\\d{2}[:.]\\d{2}\\b",
    "|\\b\\d{1,3}[:.]\\d{2}\\b",
    # Time units
    "|\\b\\d+(?:\\.\\d+)?\\s*(?:hour|hr)s?\\b",
    "|\\b\\d+(?:\\.\\d+)?\\s*(?:minute|min|mint)s?\\b",
    "|\\b\\d+(?:\\.\\d+)?\\s*(?:second|sec)s?\\b",
    # Ranges (simple) - Make non-greedy and handle spaces/hyphens, allow different formats
    "|\\b\\d{2}[:.]\\d{2}[:.]\\d{2}[:.]\\d{2}\\s*(?:to|-)\\s*\\d{2}[:.]\\d{2}[:.]\\d{2}[:.]\\d{2}\\b",
    # Percentages (often indicate reduction, not a timestamp)
    "|\\bby\\s+\\d+\\s*%",
    # Standalone numbers that might be misinterpreted (e.g., reel numbers, shot numbers) - be careful here
    "|(?<![:.\\d])\\b(?:shot|reel|sc|scene)\\s*no\\.?\\s*\\d+\\b", # Explicitly match shot/reel numbers
    "|(?<![:.\\d])\\b\\d{1,5}\\b(?![:.\\d])" # Avoid matching parts of timecodes, allow up to 5 digits, ensure not part of other numbers/times
  )
  
  # Helper function to extract tags
  extract_tags_dt <- function(text_vector, patterns) {
    sapply(text_vector, function(text) {
      if (is.na(text)) return(NA_character_)
      text <- as.character(text) # Ensure character
      text_clean <- tolower(text)
      matched_tags <- names(patterns)[map_lgl(patterns, ~ grepl(.x, text_clean, ignore.case = TRUE, perl = TRUE))]
      if(length(matched_tags) == 0) return("") #
      paste(sort(matched_tags), collapse = "; ")
    }, USE.NAMES = FALSE)
  }
  
  # Ensure description column is character
  if (!is.character(df[[description_col]])) {
    df[, (description_col) := as.character(get(description_col))]
  }
  # Create temporary raw description column (trimmed)
  temp_col_name <- paste0("temp_raw_", description_col)
  df[, (temp_col_name) := str_trim(get(description_col))]
  
  
  # Apply Tagging and Clean Description
  df[, `:=`(
    mod_tags = extract_tags_dt(get(temp_col_name), mod_patterns),
    content_tags = extract_tags_dt(get(temp_col_name), content_patterns),
    type_tags = extract_tags_dt(get(temp_col_name), type_patterns),
    # Create cleaned description: remove timestamps, collapse space, trim space/punct
    cleaned_description = map_chr(get(temp_col_name), ~ {
      if (is.na(.x)) return(NA_character_) # Handle NA input
      temp <- str_replace_all(.x, time_regex_for_removal, "") # Remove timestamps
      temp <- gsub("\\s+", " ", temp) # Collapse internal whitespace
      temp <- str_trim(temp) # Trim leading/trailing whitespace
      temp <- str_replace(temp, "^[[:punct:]]+", "") # Remove leading punct only
      temp <- str_replace(temp, "[[:punct:]]+$", "") # Remove trailing punct only
      temp <- str_trim(temp) # Final trim in case removing punct left spaces
      # Return empty string if only whitespace/punct remains, else return cleaned
      fifelse(temp == "", "", temp)
    })
  )]
  
  # Remove the temporary raw description column
  df[, (temp_col_name) := NULL]
  
  # Clean numeric cut_no if it exists
  if ("cut_no" %in% names(df)) {
    # Try converting to integer, NA if fails
    df[, cut_no := suppressWarnings(as.integer(str_trim(as.character(cut_no))))]
  }
  
  
  # Helper function to convert time columns (deleted, replaced, inserted)
  # Input expected to be character, output is numeric (minutes)
  convert_time_column <- function(time_col_vector) {
    time_char <- as.character(time_col_vector)
    time_char <- str_trim(time_char) # Trim whitespace
    
    result_mins <- fcase(
      is.na(time_char) | time_char == "", 0,
      # Format: MM.SS or M.SS or M.S (treat M.S as M.S0)
      grepl("^\\d+\\.\\d{1,2}$", time_char), {
        parts = str_split_fixed(time_char, "\\.", 2)
        mins = suppressWarnings(as.numeric(parts[,1]))
        secs_str = parts[,2]
        # Pad single digit seconds (e.g., 1.5 -> 1.50)
        secs_str = ifelse(nchar(secs_str) == 1 & !is.na(secs_str), paste0(secs_str, "0"), secs_str)
        secs = suppressWarnings(as.numeric(secs_str))
        fifelse(is.na(mins) | is.na(secs), 0, mins + secs/60)
      },
      # Format: Plain number (assume seconds if > 1000, else minutes)
      # Be cautious with this assumption - may need refinement based on data patterns
      grepl("^\\d+(\\.\\d+)?$", time_char), {
        num_val = suppressWarnings(as.numeric(time_char))
        # Heuristic: if > 1000, likely seconds; otherwise, assume minutes.
        # This might be inaccurate if there are >1000 min deletions.
        fifelse(!is.na(num_val) & num_val > 1000, num_val / 60,
                fifelse(!is.na(num_val), num_val, 0))
      },
      # Default case for unparseable formats
      default = 0
    )
    # Ensure non-negative results
    pmax(0, result_mins)
  }
  
  # Apply time conversion if columns exist
  if ("deleted" %in% names(df)) df[, deleted_mins := convert_time_column(deleted)] else df[, deleted_mins := 0]
  if ("replaced" %in% names(df)) df[, replaced_mins := convert_time_column(replaced)] else df[, replaced_mins := 0]
  if ("inserted" %in% names(df)) df[, inserted_mins := convert_time_column(inserted)] else df[, inserted_mins := 0]
  
  # Calculate total modified time (handle potential NAs in calculated mins)
  df[, total_modified_time_mins := round(fifelse(is.na(deleted_mins), 0, deleted_mins) +
                                           fifelse(is.na(replaced_mins), 0, replaced_mins) +
                                           fifelse(is.na(inserted_mins), 0, inserted_mins), 2)]
  
  # Remove original time columns if they exist
  if ("deleted" %in% names(df)) df[, deleted := NULL]
  if ("replaced" %in% names(df)) df[, replaced := NULL]
  if ("inserted" %in% names(df)) df[, inserted := NULL]
  
  return(df)
}


#' Cleans HTML/CSS, extracts basic info from film names, prioritizes language column.
#'
#' @param df Dataframe. Assumes data.table format.
#' @return Dataframe with cleaned columns.
clean_embedded_content <- function(df) {
  # Assume df is already a data.table
  result <- copy(df) # Work on a copy
  
  # Ensure key text columns exist and are character type
  cols_to_ensure_char <- c("film_name_full", "description", "language", "cleaned_description", "film_name")
  for(col in cols_to_ensure_char) {
    if (col %in% names(result)) {
      if (!is.character(result[[col]])) {
        result[, (col) := as.character(get(col))]
      }
    } else {
      # Add if missing, needed for subsequent steps
      result[, (col) := NA_character_]
    }
  }
  
  # Clean CSS/HTML from relevant text columns
  cols_to_clean_html <- c("film_name_full", "description", "cleaned_description", "film_name")
  html_css_regex <- regex("<style.*?/style>|<.*?>|qr-redirect-endorsment.*?EndorsementFile No\\.",
                          ignore_case = TRUE, dotall = TRUE)
  for(col in cols_to_clean_html) {
    if (col %in% names(result)) {
      result[, (col) := str_trim(str_replace_all(get(col), html_css_regex, ""))]
      # Replace empty strings resulting from cleaning with NA
      result[get(col) == "", (col) := NA_character_]
    }
  }
  
  
  # Identify records with embedded tables (using original description if available)
  if ("description" %in% names(result)) {
    result[, has_embedded_table := str_detect(description, regex("Cut\\s+No\\.\\s+Description.*Deleted.*Replaced.*Inserted", ignore_case = TRUE))]
    result[is.na(description), has_embedded_table := FALSE] # Set to FALSE if description is NA
  } else {
    result[, has_embedded_table := FALSE] # Default if no description column
  }
  
  
  # Extract base film name from film_name_full if available
  if ("film_name_full" %in% names(result)) {
    result[, film_base_name := str_trim(str_replace(film_name_full, "\\s*\\(.*$", ""))]
    result[film_base_name == "", film_base_name := NA_character_] # Handle cases where cleaning leaves empty string
    # If film_name exists and base name is NA, use film_name as base
    if ("film_name" %in% names(result)) {
      result[is.na(film_base_name) & !is.na(film_name), film_base_name := film_name]
    }
  } else if ("film_name" %in% names(result)) {
    # Fallback to film_name if film_name_full doesn't exist
    result[, film_base_name := film_name]
  } else {
    result[, film_base_name := NA_character_]
  }
  
  
  # --- Language Extraction (Revised Logic) ---
  # Use existing 'language' column first. If missing, try extracting from 'film_name_full'.
  result[, primary_language := NA_character_] # Initialize
  
  if ("language" %in% names(result)) {
    # Use existing language if not NA or empty string
    result[!is.na(language) & str_trim(language) != "", primary_language := str_trim(language)]
  }
  
  # If primary_language is still NA, try extracting from film_name_full
  if ("film_name_full" %in% names(result)) {
    result[is.na(primary_language), primary_language := {
      fnf_sub <- film_name_full[is.na(primary_language)]
      if(length(fnf_sub) == 0) {
        # If no rows need processing, return vector of NAs of correct length
        rep(NA_character_, length(is.na(primary_language)))
      } else {
        lang_pattern <- "\\(([^()]+?)(?:\\s+WITH\\s+.*?)?\\)" # Pattern to find text in parentheses
        extracted <- str_match(fnf_sub, lang_pattern)
        potential_lang <- fifelse(!is.na(extracted[, 2]), str_trim(extracted[, 2]), NA_character_)
        
        # Heuristics to filter out non-language entries (e.g., format, subtitles info)
        is_likely_format_or_other <- grepl("\\d|format|color|&|\\b(2d|3d|scope|screen|b/w|with|subtitle|english subtitle|don't love|part|ver|version)\\b",
                                           potential_lang, ignore.case = TRUE) | nchar(potential_lang) > 20 # Also filter long strings
        
        fifelse(!is.na(potential_lang) & !is_likely_format_or_other, potential_lang, NA_character_)
      }
    }]
  }
  
  # Final cleanup for language columns (convert to factor later in main workflow)
  if ("primary_language" %in% names(result)) {
    result[primary_language == "", primary_language := NA_character_]
  }
  if ("language" %in% names(result)) {
    result[language == "", language := NA_character_]
  }
  
  
  return(result)
}


#' Main function to orchestrate data cleaning, joining, and saving.
#'
#' @param modifications_raw_df Raw modifications data.table.
#' @param metadata_raw_df Raw metadata data.table.
#' @param output_cleaned_path Path to save the final cleaned Parquet file.
#' @return A list containing:
#'   - final_data: The main cleaned and joined data.table.
#'   - cleaned_meta: The cleaned metadata data.table (before join).
#'   - cleaned_mods: The cleaned modifications data.table (before join).
main <- function(modifications_raw_df, metadata_raw_df, output_cleaned_path) {
  
  print("Performing initial cleaning on metadata...")
  metadata_cleaned <- clean_metadata(metadata_raw_df) # Pass the raw df
  
  print("Performing initial cleaning on modifications...")
  if (!"description" %in% names(modifications_raw_df)) {
    warning("Column 'description' not found in modifications data. Modification cleaning partially skipped.")
    modifications_cleaned <- copy(modifications_raw_df) # Still copy
    # Ensure certificate_id is cleaned even if description is missing
    if ("certificate_id" %in% names(modifications_cleaned)) {
      if(!is.character(modifications_cleaned$certificate_id)) modifications_cleaned[, certificate_id := as.character(certificate_id)]
      modifications_cleaned[, certificate_id := str_trim(certificate_id)]
      modifications_cleaned[, certificate_id := fifelse(certificate_id == "0", "0", sub("^0+", "", certificate_id))]
      modifications_cleaned[is.na(certificate_id), certificate_id := NA_character_]
    } else {
      stop("Critical column 'certificate_id' not found in modifications data.")
    }
    # Add expected columns as NA/0 if description processing was skipped
    mod_cols_to_add <- c("mod_tags", "content_tags", "type_tags", "cleaned_description")
    for(mc in mod_cols_to_add) if(!mc %in% names(modifications_cleaned)) modifications_cleaned[, (mc) := NA_character_]
    num_cols_to_add <- c("deleted_mins", "replaced_mins", "inserted_mins", "total_modified_time_mins")
    for(nc in num_cols_to_add) if(!nc %in% names(modifications_cleaned)) modifications_cleaned[, (nc) := 0]
  } else {
    modifications_cleaned <- clean_modifications(modifications_raw_df, description_col = "description") # Pass the raw df
  }
  
  # ID columns should now be clean character strings from the cleaning functions.
  # Ensure they are character type before join (redundant but safe check).
  if ("id" %in% names(metadata_cleaned) && !is.character(metadata_cleaned$id)) {
    warning("Metadata 'id' column was not character type before join. Attempting conversion.")
    metadata_cleaned[, id := as.character(id)]
  }
  if ("certificate_id" %in% names(modifications_cleaned) && !is.character(modifications_cleaned$certificate_id)) {
    warning("Modifications 'certificate_id' column was not character type before join. Attempting conversion.")
    modifications_cleaned[, certificate_id := as.character(certificate_id)]
  }
  
  print("Joining modifications and metadata...")
  if (!"id" %in% names(metadata_cleaned) || !"certificate_id" %in% names(modifications_cleaned)) {
    stop("Required ID columns ('id' in metadata, 'certificate_id' in modifications) not found for join.")
  }
  
  # Define columns to select from each table for the join
  # Prioritize columns less likely to cause type issues in join itself
  cols_from_meta_minimal <- c("id", "film_name", "film_name_full", "language", "duration_mins",
                              "cert_date_parsed", "cert_no", "category", "format", "applicant", "certifier")
  cols_from_meta_exist <- intersect(cols_from_meta_minimal, names(metadata_cleaned))
  
  cols_from_mods_minimal <- names(modifications_cleaned) # Take all cleaned mod columns
  cols_from_mods_exist <- intersect(cols_from_mods_minimal, names(modifications_cleaned)) # Should be all
  
  # Perform the left join
  censorship_data <- merge(
    modifications_cleaned[, ..cols_from_mods_exist],
    metadata_cleaned[, ..cols_from_meta_exist],
    by.x = "certificate_id",
    by.y = "id",
    all.x = TRUE # Keep all modification rows, fill metadata with NA if no match
  )
  print(paste("Rows after join:", nrow(censorship_data)))
  
  print("Applying post-join cleaning (HTML, language extraction)...")
  censorship_data <- clean_embedded_content(censorship_data)
  
  
  print("Consolidating potentially duplicated metadata within certificate IDs...")
  metadata_cols_to_consolidate <- c("film_name", "film_base_name", "film_name_full", "language",
                                    "primary_language", "duration_mins", "cert_date_parsed", "cert_no",
                                    "category", "format", "applicant", "certifier")
  metadata_cols_exist <- intersect(metadata_cols_to_consolidate, names(censorship_data))
  
  if (length(metadata_cols_exist) > 0 && "certificate_id" %in% names(censorship_data)) {
    # Use `na.omit` and take the first non-NA value within each group
    for (col in metadata_cols_exist) {
      # Check if column exists before trying to access
      if(col %in% names(censorship_data)) {
        censorship_data[, (col) := {
          vals <- na.omit(get(col)) # Get all non-NA values for this column within the group
          # If there's at least one non-NA value, use the first one; otherwise, keep NA
          # Use .SD[[col]][1] to preserve the original type if all were NA
          if (length(vals) > 0) vals[[1]] else .SD[[col]][1]
        }, by = certificate_id]
      }
    }
    print("Metadata consolidation complete.")
  } else {
    warning("Could not consolidate metadata: 'certificate_id' or metadata columns missing after join/cleaning.")
  }
  
  # Rows that are identical across certificate ID and the *cleaned* modification description
  print("Removing duplicate modification rows (same certificate_id and cleaned_description)...")
  if ("certificate_id" %in% names(censorship_data) && "cleaned_description" %in% names(censorship_data)) {
    # Handle cases where cleaned_description might be NA - treat NA as a distinct value for uniqueness
    # Replace NA temporarily for `unique` check if needed, or rely on data.table handling
    # data.table unique treats NA as distinct by default
    original_rows <- nrow(censorship_data)
    dedup_cols <- c("certificate_id", "cleaned_description")
    censorship_data <- unique(censorship_data, by = dedup_cols)
    rows_removed <- original_rows - nrow(censorship_data)
    print(paste("Removed", rows_removed, "duplicate modification rows."))
  } else {
    warning("Could not remove duplicates: 'certificate_id' or 'cleaned_description' column missing.")
  }
  
  
  print("Selecting and reformatting final columns...")
  final_cols_desired <- c(
    "certificate_id", "film_base_name", "film_name_full",
    "primary_language",
    "duration_mins",
    "mod_tags", "content_tags", "type_tags",
    "description", # Keep original modification description
    "cleaned_description", # Keep cleaned modification description
    "cut_no", "deleted_mins", "replaced_mins", "inserted_mins", "total_modified_time_mins",
    "cert_date_parsed", "cert_no",
    # "category", "format", # Explicitly excluding these as per original script
    "applicant", "certifier"
    # "has_embedded_table" # Excluded as likely intermediate flag
  )
  # Select only columns that actually exist in the dataframe
  final_cols_exist <- intersect(final_cols_desired, names(censorship_data))
  censorship_data <- censorship_data[, ..final_cols_exist]
  
  
  # Rename date column for clarity
  if ("cert_date_parsed" %in% names(censorship_data)) {
    setnames(censorship_data, "cert_date_parsed", "cert_date")
    setnames(censorship_data, "film_base_name", "film_name")
    setnames(censorship_data, "primary_language", "language")
  }
  
  
  # Convert specific columns to factor (final check)
  factor_cols <- c("mod_tags", "content_tags", "type_tags", "language", "primary_language") 
  for (col in factor_cols) {
    if (col %in% names(censorship_data)) {
      # Check if not already factor and has non-NA values before converting
      if (!is.factor(censorship_data[[col]]) && any(!is.na(censorship_data[[col]]))) {
        censorship_data[, (col) := as.factor(get(col))]
      } else if (all(is.na(censorship_data[[col]]))) {
        # If all NA, ensure it's character NA before potential factor conversion attempt
        censorship_data[, (col) := NA_character_]
      }
    }
  }
  
  # Ensure numeric types (final check) - primarily for calculated fields
  numeric_cols <- c("duration_mins", "deleted_mins", "replaced_mins", "inserted_mins",
                    "total_modified_time_mins", "cut_no")
  for (col in numeric_cols) {
    if (col %in% names(censorship_data)) {
      if (!is.numeric(censorship_data[[col]])) {
        # Attempt conversion, coerce errors to NA
        censorship_data[, (col) := suppressWarnings(as.numeric(get(col)))]
      }
    }
  }
  
  # Ensure date type
  if ("cert_date" %in% names(censorship_data) && !inherits(censorship_data$cert_date, "Date")) {
    warning("Final 'cert_date' column is not Date type. Check parsing.")
  }
  
  
  # Filter based on duration to KEEP ONLY MOVIES
  if ("duration_mins" %in% names(censorship_data)) {
    original_rows_filter <- nrow(censorship_data)
    censorship_data <- censorship_data[is.na(duration_mins) | duration_mins >= 60]
    rows_filtered <- original_rows_filter - nrow(censorship_data)
    if(rows_filtered > 0) print(paste("Filtered out", rows_filtered, "rows with duration_mins <= 1."))
  }
  
  print("Saving final processed data...")
  output_dir <- dirname(output_cleaned_path)
  if (!dir.exists(output_dir)) {
    print(paste("Creating output directory:", output_dir))
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Check for list columns before saving (shouldn't happen with data.table ops)
  list_cols <- names(which(sapply(censorship_data, is.list)))
  if(length(list_cols) > 0) {
    warning("List columns found before saving: ", paste(list_cols, collapse=", "), ". This is unexpected. Check data processing steps.")
    # Attempt basic conversion to character string
    for(col in list_cols) {
      censorship_data[, (col) := sapply(get(col), function(x) {
        if(is.null(x) || length(x) == 0) return(NA_character_)
        paste(unlist(x), collapse="; ")
      })]
    }
  }
  
  tryCatch({
    print(paste("Attempting to save Parquet to:", output_cleaned_path))
    write_parquet(censorship_data, output_cleaned_path, compression = "zstd", compression_level = 5)
    print(paste("Cleaned data saved successfully as Parquet to:", output_cleaned_path))
  }, error = function(e) {
    print(paste("Error saving Parquet file:", e$message))
    warning("Failed to save the final data as Parquet.")
  })
  
  # Derive CSV path from Parquet path
  output_csv_path <- sub("\\.parquet$", ".csv", output_cleaned_path)
  tryCatch({
    print(paste("Attempting to save CSV to:", output_csv_path))
    # Use fwrite for efficiency with data.tables
    fwrite(censorship_data, output_csv_path, row.names = FALSE, na = "") # Write NA as empty string
    print(paste("Cleaned data saved successfully as CSV to:", output_csv_path))
  }, error = function(e) {
    print(paste("Error saving CSV file:", e$message))
    warning("Failed to save the final data as CSV.")
  })
  
  
  print("Workflow complete.")
  # Return a list containing key data frames for debugging
  return(list(
    final_data = censorship_data,
    cleaned_meta = metadata_cleaned,
    cleaned_mods = modifications_cleaned
  ))
}


print("Loading raw data...")
if (!file.exists(raw_modifications_path)) stop("Raw modifications file not found at:", raw_modifications_path)
if (!file.exists(raw_metadata_path)) stop("Raw metadata file not found at:", raw_metadata_path)

modifications_raw <- tryCatch({
  fread(raw_modifications_path, colClasses=c(certificate_id="character"), na.strings = c("", "NA", "N/A"))
}, error = function(e) {
  stop("Failed to load modifications data: ", e$message)
})
print(paste("Loaded", nrow(modifications_raw), "rows from modifications data."))

metadata_raw <- tryCatch({
  fread(raw_metadata_path, colClasses=c(id="character"), na.strings = c("", "NA", "N/A"))
}, error = function(e) {
  stop("Failed to load metadata data: ", e$message)
})
print(paste("Loaded", nrow(metadata_raw), "rows from metadata data."))


results_list <- main(
  modifications_raw_df = modifications_raw,
  metadata_raw_df = metadata_raw,
  output_cleaned_path = output_cleaned_path
)


results_df <- results_list$final_data


# }