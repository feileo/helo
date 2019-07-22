from trod import utils

# from functools import wraps


def load(model, result_dict, use_td=False):

    if isinstance(result_dict, utils.Tdict):
        if not result_dict:
            return utils.Tdict()
        return _do_load(result_dict, model)
    # elif isinstance(result_dict, (list, tuple)):
    # if not result_dict:
    #     return FetchResult([], model)
    fe = FetchResult()
    for rd in result_dict:
        fe.append(_do_load(rd))
    return fe


def _do_load(result_dict, model):

    if not isinstance(result_dict, utils.Tdict):
        raise ValueError(f'Invalid loader data: {result_dict}')
    model = model()
    for key, value in result_dict.items():
        model.set_value(key, value, is_loader=True)
    return model


class FetchResult(list):

    # def __init__(self, result_dict, model):
    #     super().__init__()

    #     self._result_dict = result_dict

    #     # self._model = model
    #     # self._use_troddict = False

    def __repr__(self):
        pass

    __str__ = __repr__

    def __iter__(self):
        """ for x in self """
        pass

    def __getitem__(self, idx):
        """ self[key] """
        pass

    def __contains__(self, value):
        """ value in self, value not in self """
        pass


class ExecResults:
    def __init__(self, affected, last_id):
        self.affected = affected
        self.last_id = last_id

    def __repr__(self):
        pass

    __str__ = __repr__
