from datetime import timedelta, datetime


def parse_duration(duration_str):
    try:
        duration_str = duration_str.replace("P", "").replace("T", "")

        days = hours = minutes = seconds = 0

        if "D" in duration_str:
            days_part, duration_str = duration_str.split("D", 1) 
            days = int(days_part)

        if "H" in duration_str:
            hours_part, duration_str = duration_str.split("H", 1) 
            hours = int(hours_part)

        if "M" in duration_str:
            minutes_part, duration_str = duration_str.split("M", 1)
            minutes = int(minutes_part)

        if "S" in duration_str:
            seconds_part, duration_str = duration_str.split("S", 1)
            seconds = int(seconds_part)

        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    except Exception as e:
        raise ValueError(f"Invalid duration format: {duration_str}") from e
    

def transform_data(row):

    duration_time_delta = parse_duration(row["Duration"])
    row["Duration"] = (datetime.min + duration_time_delta).time()
    row["Video_Type"] = "Shorts" if duration_time_delta.total_seconds() <= 60 else "Regular"
    return row