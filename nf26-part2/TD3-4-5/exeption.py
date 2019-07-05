class InsertError(Exception):
    """Raised when you can't insert data in table"""
    pass

class ReadError(Exception):
     """Raised when you can't read all the data"""
     pass