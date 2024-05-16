
class XLSX:

    def __init__(self) -> None:
        pass

    def write(self, buffer: bytes, name: str) -> bool:

        try:
            with open(f"{name}.xlsx", "wb") as file:
                file.write(buffer)
            return True
        except BufferError as err:
            raise err from err
        except Exception as err:
            raise err from err
