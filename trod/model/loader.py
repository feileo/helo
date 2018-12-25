
class Loader:

    def __init__(self, model, data):
        self.model = model
        self.data = data

    def load(self):
        if not self.data:
            return self.model()
        if isinstance(self.data, dict):
            return self._do_load(self.data)
        elif isinstance(self.data, (list, tuple)):
            res_models = []
            for data in self.data:
                res_models.append(self._do_load(data))
            return res_models

    def _do_load(self, data):
        if isinstance(data, dict):
            raise ValueError('Invalid loader data: {}'.format(type(data)))
        res_model = self.model()
        for key, value in data.items():
            res_model.set_value(key, value)
        return res_model
