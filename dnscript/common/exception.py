class CommonException(Exception):
    function_name = None
    title = None

    def __init__(self, *args, **kwargs):
        self.function_name = kwargs.pop('function_name', '')
        self.title = self.__class__.__name__

    def __str__(self):
        return f'[{self.title}] - {self.function_name}'


class AccessViolation(CommonException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_code = kwargs['client_code']

    def __str__(self):
        return super().__str__() + f' - {self.client_code}'


class DBSessionError(CommonException):
    pass


class DataFetchError(CommonException):
    kwargs = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        kwargs.pop('function_name', '')

        self.kwargs = kwargs

    def __str__(self):
        return super().__str__() + f' - {str(self.kwargs)}'


class DataUpdateError(CommonException):
    kwargs = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        kwargs.pop('function_name', '')

        self.kwargs = kwargs

    def __str__(self):
        return super().__str__() + f' - {str(self.kwargs)}'


class APIError(CommonException):
    pass


class ControllerError(CommonException):
    pass
