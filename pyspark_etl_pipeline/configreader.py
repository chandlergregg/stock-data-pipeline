from configparser import ConfigParser

class ConfigReader:
    """
    Reads configuration file into config dictionary
    """
    def __init__(self, filename, section):
        self.filename = filename
        self.section = section

    def get_config(self):
        """
        Build config dictionary from file and return
        """
        parser = ConfigParser()
        parser.read(self.filename)

        config = {}
        if parser.has_section(self.section):
            params = parser.items(self.section)
            for param in params:
                config[param[0]] = param[1].strip('"')
        else:
            raise Exception(f"Section {self.section} not found in the {self.filename} file")
        
        return config