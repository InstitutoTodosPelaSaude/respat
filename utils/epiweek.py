from datetime import datetime, timedelta
from epiweeks import Week

def get_epiweek_str(
    datetime_: datetime = datetime.now(),
    format: str = '{EPINUM}',
    zfill: int = 0,
    use_epiweek_enddate: bool = True
):
    """ Create a string with the epiweek number from 'datetime_'.
    Use {EPINUM} to show where you want to insert the epiweek number
    and use all the datetime formatters to format the string.

    Example: format = '%Y-%m-%d-{EPINUM}'

    Args:
        datetime_ (datetime): The datetime to get the epiweek number.
        format (str): The format to show epiweek number.
        zfill (int): The total length of the resulting string, padded with leading zeros if necessary.

    Returns:
        str: The formatted string with the epiweek number.
    """
    assert type(datetime_) == datetime, f"'datetime_' must be a datetime, not {type(datetime_)}."
    assert type(format) == str, f"'format' must be a str, not {type(format)}."
    assert "{EPINUM}" in format, "'format' must have '{EPINUM}' keywork to be replaced by the epiweek number."
    assert type(zfill) == int, f"'zfill' must be a int, not {type(zfill)}."
    assert type(use_epiweek_enddate) == bool, f"'use_epiweek_enddate' must be a bool, not {type(use_epiweek_enddate)}."

    # Move datetime_ to epiweek enddate, moving for the next saturday.
    if use_epiweek_enddate:
        days_until_saturday = (5 - datetime_.weekday()) % 7  # Saturday is 5
        datetime_ = datetime_ + timedelta(days=days_until_saturday)

    # Create string for the datetime format keeping the '{EPINUM}' keyword
    datetime_str = datetime_.strftime(format)

    # Get the epiweek number from datetime
    epiweek_num = Week.fromdate(datetime_, system="iso").weektuple()[1]

    # Replace '{EPINUM}' keyword by the epiweek number
    epiweek_str = str(epiweek_num).zfill(zfill)
    result = datetime_str.replace('{EPINUM}', epiweek_str)

    return result
