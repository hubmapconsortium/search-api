from flask import Blueprint, request, Response, abort, jsonify
import src.libs.assay_type as assay_type_module
from opensearch_helper_functions import *

hubmap_blueprint = Blueprint('hubmap_blueprint', __name__)


@hubmap_blueprint.route('/assaytype', methods=['GET'])
def __assaytypes():
    return assaytypes()

@hubmap_blueprint.route('/assaytype/<name>', methods=['GET'])
@hubmap_blueprint.route('/assayname', methods=['POST'])
def __assayname(name=None):
    return assayname(name)


# @hubmap_blueprint.route('/testmethod', methods=['GET'])
# def testmethod():
#     return "Test method successful"


####################################################################################################
## Assay type API
####################################################################################################

def assaytypes():
    primary = None
    simple = False
    for key, val in request.args.items():
        if key == 'primary':
            primary = val.lower() == "true"
        elif key == 'simple':
            simple = val.lower() == "true"
        else:
            abort(400, f'invalid request parameter {key}')

    if primary is None:
        name_l = [name for name in assay_type_module.AssayType.iter_names()]
    else:
        name_l = [name for name in assay_type_module.AssayType.iter_names(primary=primary)]

    if simple:
        return jsonify(result=name_l)
    else:
        return jsonify(result=[assay_type_module.AssayType(name).to_json() for name in name_l])


def assayname(name=None):
    if name is None:
        if not request.is_json:
            bad_request_error("A JSON body and appropriate Content-Type header are required")
        try:
            name = request.json['name']
        except Exception:
            abort(400, 'request contains no "name" field')
    try:
        return jsonify(assay_type_module.AssayType(name).to_json())
    except Exception as e:
        abort(400, str(e))
