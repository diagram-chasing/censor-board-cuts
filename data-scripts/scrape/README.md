# CBFC Scraper

This is a Python script that scrapes data from the CBFC website.

## Usage

```bash
python main.py
```

## Notes

### Understanding Certificate IDs

Each certificate ID is a 16-digit number structured as follows:

1. The first 4 digits are always "1000"
2. The 5th digit indicates the CBFC regional office (see region codes below)
3. The 6th digit is always "0"
4. Digits 7-10 represent the year (add 900 to the year)
5. The last 8 digits are sequential numbers

CBFC Regional Office Codes:

- 1 = Mumbai
- 2 = Bangalore
- 3 = Chennai
- 4 = Cuttack
- 5 = Delhi
- 6 = Guwahati
- 7 = Hyderabad
- 8 = Kolkata
- 9 = Thiruvananthapuram

Example, the certificate ID `100090292400000109`:

- "9" in position 5 indicates Thiruvananthapuram regional office
- "2924" in positions 7-10 represents year 2024 (2024 + 900 = 2924)

## What IDs?

For now, we scrape from 1 to infinity for each combination of state+year, and terminate the scraping for a state+year after encountering consecutive IDs that turn out to be invalid. A little hacky! Unfortunately, there's no database of valid IDs so this brute force approach is what we're stuck with.
