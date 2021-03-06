import logging
import os


def clear_paragraph(paragraph):
    """
    clear paragraph from p balise
    :param paragraph:
    :type paragraph:
    :return:
    :rtype:
    """
    paragraph = paragraph.replace('<p>', '')
    paragraph = paragraph.replace('</p>', '')
    paragraph = paragraph.replace("\u200b", '').strip()
    return paragraph


def get_source_date(sources):
    """
    clear and split to get source and date separately in two str
    :param sources: paragraphe of source and date
    :type sources: str
    :return: name, date of source
    :rtype: str, str
    """

    # Clear str
    sources = clear_paragraph(sources)
    sources = sources.replace('<strong>', '')
    sources = sources.replace('</strong>', '')

    # split str to a list to get date and source separately
    list_sources = sources.split(',')
    source_name = list_sources.pop().replace("\u200b", '').strip()
    date_source = list_sources[-1].strip()

    return source_name, date_source


def get_country_list(str_input):
    """

    :param str_input:
    :type str_input:
    :return:
    :rtype:
    """
    # Clear str
    country = clear_paragraph(str(str_input))

    # Get country list
    country_list = country.split("<br/>")
    return country_list


