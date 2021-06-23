import configparser as ConfigParser
import logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

def parse_config():
    p = ConfigParser.RawConfigParser()
    options = {}
    rcfile = './config.conf'
    try:
        p.read([rcfile])
        for item in p.items():
            for (name, value) in p.items(item[0]):
                options.setdefault(item[0], {})
                options[item[0]][name] = value
    except IOError as ioerr:
        raise ioerr
    except ConfigParser.NoSectionError:
        pass

    return options


options = parse_config()
