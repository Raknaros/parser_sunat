import re
import codecs

def get_xml_encoding(file_path: str, default_encoding='utf-8') -> str:
    """
    Lee los primeros bytes de un archivo para detectar el encoding especificado
    en la declaración XML.
    """
    try:
        with open(file_path, 'rb') as f:
            start_of_file = f.read(1024)

        head = start_of_file.decode('latin-1')

        # Expresión regular corregida.
        match = re.search(r"<\?xml.*encoding\s*=\s*['\"](.*?)['\"]", head, re.IGNORECASE)
        
        if match:
            encoding = match.group(1).strip()
            try:
                codecs.lookup(encoding)
                return encoding
            except LookupError:
                return default_encoding
    except (IOError, IndexError):
        pass
    
    return default_encoding