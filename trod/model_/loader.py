from trod import utils


def load(results, model, use_tdict):

    if not results:
        return _empty(results, model, use_tdict)

    if use_tdict:
        return utils.formattdict(results)
    return _load_to_model(results, model)


def _empty(results, model, use_tdict):
    if isinstance(results, dict):
        if use_tdict:
            return utils.Tdict()
        return model()
    if isinstance(results, (list, tuple)):
        if use_tdict:
            return [utils.Tdict()]
        return FetchResult()

    raise ValueError()


def _load_to_model(results, model):

    # TODO func field
    def _do(results, model):
        model = model()
        for key, value in results.items():
            model.set_value(key, value, is_loader=True)
        return model

    if isinstance(results, dict):
        return _do(results, model)
    if isinstance(results, (list, tuple)):
        return FetchResult([_do(r, model) for r in results])

    raise ValueError()


class FetchResult(list):

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
