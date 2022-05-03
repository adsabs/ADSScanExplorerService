from flask import Blueprint, jsonify, current_app, request, redirect
from presentation_api.extensions import manifest_factory
from iiif_prezi.factory import Annotation
import json
from presentation_api.models import Article, Page


# define the blueprint
bp_search= Blueprint('search', __name__)

@bp_search.route('/<string:article_id>/search', methods=['GET'])
def search(article_id : str):
    with current_app.session_scope() as session:
        article = session.query(Article).filter_by(id=article_id).first()
        query = request.args.get('q')
        if article and query: 
            
            # TODO: Here we should perform a search in the OCR text.
            # Below code is an hard coded example.

            annotation_list = manifest_factory.annotationList(request.url)
            annotation_list.resources = []

            annotation : Annotation = annotation_list.annotation('http://example.org/identifier/annotation/anno-line')
            annotation.text('near-infrared')

            page = session.query(Page).filter_by(article_id=article.id).first()

            # annotation.on should point to the canvas id
            canvas_id = f'{current_app.config.get("BASE_URL")}/{article.name}/canvas/{page.id}.json'
            annotation.on = "".join([canvas_id, "#xywh=670,950,700,182"])

            return jsonify(json.loads(annotation_list.toString(compact=True)))