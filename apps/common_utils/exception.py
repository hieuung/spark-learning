class APIError(Exception):
    def to_json(self):
        return {'ok': False, 'message': str(self)}


class APIMessageError(APIError):
    def __init__(self, message, code=None):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message

    def to_json(self):
        ret = super(APIMessageError, self).to_json()

        if self.code:
            ret['Code'] = self.code

        return ret


class NotAuthenticatedError(APIError):
    """Not logged in"""
    status_code = 401

    def __init__(self, reason=None):
        self.reason = reason

    def to_response(self):
        return dict(
            response={
                'ok': False,
                'message': 'Not Authenticated',
                'reason': self.reason
            },
            status=401,
        )

    to_json = to_response


class UnAuthorizedError(APIError):
    """Logged in but not authorized to access the resource"""
    status_code = 401

    def __init__(self, reason=None):
        self.reason = reason

    def to_response(self):
        return dict(
            response={
                'ok': False,
                'message': 'Not Authorized',
                'reason': self.reason
            },
            status=self.status_code,
        )

    to_json = to_response
