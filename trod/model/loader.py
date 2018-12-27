
class Loader:

    def __init__(self, model, data):
        self.model = model
        self.data = data

    def load(self):
        if not self.data:
            return None
        if isinstance(self.data, (list, tuple)):
            res_models = []
            for data in self.data:
                res_models.append(self._do_load(data))
            return res_models
        else:
            return self._do_load(self.data)

    def _do_load(self, data):
        if not isinstance(data, dict):
            raise ValueError('Invalid loader data: {}'.format(type(data)))
        res_model = self.model()
        for key, value in data.items():
            res_model._set_value(key, value, is_loader=True)
        return res_model
