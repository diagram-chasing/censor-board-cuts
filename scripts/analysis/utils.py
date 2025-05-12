def cleanup_language(language):
    language = language.title()
    language = language.split('with')[0].strip()
    language = language.split('With')[0].strip()
    language = language.split('(')[0].strip()
    language = language.lstrip(' ').rstrip(' ')
    language = language.replace('Partly', 'partly')

    return language

def cleanup_movie_name(movie_name):
    movie_name = movie_name.replace(' (DUBBED FRESH)', '')
    movie_name = movie_name.replace(' (FRESH DUBBED)', '')
    movie_name = movie_name.replace(' (FRESH DUB)', '')
    movie_name = movie_name.replace(' (DUBBED)', '')

    movie_name = movie_name.replace(' - DUBBED FRESH', '')
    movie_name = movie_name.replace(' - FRESH DUBBED', '')
    movie_name = movie_name.replace(' - FRESH DUB', '')
    movie_name = movie_name.replace(' - DUBBED', '')

    movie_name = movie_name.replace('(DUBBED)', '')
    movie_name = movie_name.replace(' (DUBDDED)', '')
    movie_name = movie_name.replace('    ( DUBBED)', '')
    movie_name = movie_name.replace(' (FRESS DUBBED)', '')
    movie_name = movie_name.replace(' FRESH DUBBED', '')
    movie_name = movie_name.replace('  (DUBBING)  (REVISED)', '')
    movie_name = movie_name.replace(' ( DUBBED)(REVISED)', '')
    movie_name = movie_name.replace(' ( DUBBED)', '')
    movie_name = movie_name.replace(' (FRESH) (DUB)', '')
    movie_name = movie_name.replace(' (FRESH) (DUBB)', '')
    movie_name = movie_name.replace(' (DUB)', '')
    movie_name = movie_name.replace(' (DUBB)', '')
    movie_name = movie_name.replace('(DUB)', '')
    movie_name = movie_name.replace(' (DUBBED )', '')
    movie_name = movie_name.replace(' (Dubbed)', '')
    movie_name = movie_name.replace(' _ DUBBED', '')
    movie_name = movie_name.replace(' ( DUBBED )', '')
    movie_name = movie_name.replace('( FRESH DUB)', '')

    movie_name = movie_name.replace(' (HINDI DUBBED)', '')
    movie_name = movie_name.replace(' (HINDI DUB)', '')
    movie_name = movie_name.replace('ENGLISH DUBBED', '')
    movie_name = movie_name.replace(' (TELUGU DUBBED)', '')
    movie_name = movie_name.replace(' KANNADA DUBBED VERSION', '')
    movie_name = movie_name.replace(' - MALAYALAM DUBBED FROM TAMIL', '')
    movie_name = movie_name.replace(' (MALAYALAM DUBBED VERSION-MASTERPIECE)', '')
    movie_name = movie_name.replace(' (MALAYALAM DUBBED MOVIE PATTOM POLE)', '')
    movie_name = movie_name.replace('( DUBBED FROM VIVEGAM TAMIL FILM )', '')
    movie_name = movie_name.replace(' DUBBED FROM TAMIL[KALATHUR GRAMAM]', '')
    movie_name = movie_name.replace(' DUBBED FROM TAMIL VARALARU', '')
    movie_name = movie_name.replace('- TELUGU FILM DUBBED FROM TAMIL TITLE "ORU NAAL KOOTHU"', '')
    movie_name = movie_name.replace(' - MALAYALAM DUBBED', '')

    if "(DUBBED" in movie_name:
        movie_name = movie_name.split('(DUBBED')[0].rstrip()

    return movie_name