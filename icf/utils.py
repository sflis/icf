import math
import inspect


def get_si_prefix(value: float) -> (float,str):
    """ Returns the given value to the closest si prefix and
        the correct si prefix

        example:
        >>print("The output of the powerplant is {}{}W".format(*get_si_prefix(3.3e6)))
        >>The output of the powerplant is 3.3MW

    Args:
        value (float): Value to be

    Returns:
        float, str: value rounded to nearest si prefix, corresponding si prefix
    """
    prefixes = [
        "a",
        "f",
        "p",
        "n",
        "μ",
        "m",
        "",
        "k",
        "M",
        "G",
        "T",
        "P",
        "E",
        "Z",
        "Y",
    ]
    if abs(value) < 1e-18:
        return 0, ""
    i = int(math.floor(math.log10(abs(value))))
    i = int(i / 3)
    p = math.pow(1000, i)
    s = round(value / p, 2)
    ind = i + 6
    return s, prefixes[ind]


def get_attritbues(obj_):
    """Summary

     Args:
         obj_ (TYPE): Description

     Returns:
         TYPE: Description
     """
    attributes = {}
    for attr in dir(obj_):
        if attr[0] == "_" or attr[:2] == "__":
            continue
        if inspect.ismethod(getattr(obj_, attr)) or inspect.isfunction(
            getattr(obj_, attr)
        ):
            continue
        attributes[attr] = getattr(obj_, attr)
    return attributes


def get_utc_timestamp()->(int,int):
    """Get a unix utc timestamp as a (second,nano second) tuple

    Returns:
        int, int: seconds, nano seconds
    """
    from datetime import datetime

    timestamp = datetime.utcnow().timestamp()
    s = int(timestamp)
    ns = int((timestamp - s) * 1e9)
    return s, ns


