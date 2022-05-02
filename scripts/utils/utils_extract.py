def get_source_date(sources):

    """
    clear and split to get source and date separately in two str
    :param sources: paragraphe of source and date
    :type sources: str
    :return: name, date of source
    :rtype: str, str
    """

    # Clear str
    sources = sources.replace('<p>', '')
    sources = sources.replace('</p>', '')
    sources = sources.replace('<strong>', '')
    sources = sources.replace('</strong>', '')

    # split str to a list to get date and source separately
    list_sources = sources.split(',')
    source_name = list_sources.pop().replace("\u200b",'').strip()
    date_source = list_sources[-1].strip()

    return source_name, date_source
