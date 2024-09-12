from typing import Dict
from iiif_prezi.factory import ManifestFactory, Sequence, Canvas, Image, Annotation, Manifest, Range
from scan_explorer_service.models import Article, Page, Collection
from typing import Union
from itertools import chain

class ManifestFactoryExtended(ManifestFactory):
    """ Extended manifest factory.

    Extension of the iiif_prezi manifest factory with helper 
    functions used to create manifest objects from model.
    """

    def create_manifest(self, item: Union[Article, Collection]):
        manifest = self.manifest(
            ident=f'{item.id}/manifest.json', label=item.id)
        manifest.description = item.id
        manifest.add_sequence(self.create_sequence(item))
        for range in self.create_range(item):
            manifest.add_range(range)
        return manifest

    def create_sequence(self, item: Union[Article, Collection]):
        sequence: Sequence = self.sequence()
        for page in item.pages:
            sequence.add_canvas(self.get_or_create_canvas(page)) 
        return sequence

    def create_range(self, item: Union[Article, Collection]):
        if isinstance(item, Collection):
            return list(chain(*[self.create_range(article) for article in item.articles]))

        range: Range = self.range(ident=item.bibcode, label=item.bibcode)
        for page in item.pages:
            range.add_canvas(self.get_or_create_canvas(page))
        return [range]

    def get_canvas_dict(self) -> Dict[str, Canvas]:
        if not hasattr(self, 'canvas_dict'):
            self.canvas_dict = {}
        return self.canvas_dict

    def get_or_create_canvas(self, page: Page):
        canvas_dict = self.get_canvas_dict()
        if(page.id in canvas_dict.keys()):
            return canvas_dict[page.id]
        canvas: Canvas = self.canvas(ident=str(page.id), label=f'p. {page.label}')

        if len(page.articles) > 0:
            metadata = {
                'Abstract': ''.join(f'<a href="https://ui.adsabs.harvard.edu/abs/{str(x.bibcode)}/abstract">{str(x.bibcode)}</a><br/>' for x in page.articles)
            }
            canvas.set_metadata(metadata)

        canvas.height = page.height
        canvas.width = page.width
        annotation = self.create_image_annotation(page)
        annotation.on = canvas.id
        canvas.add_annotation(annotation)
        canvas_dict[page.id] = canvas

        return canvas

    def create_image_annotation(self, page: Page):
        annotation: Annotation = self.annotation(ident=str(page.id))
        image: Image = annotation.image(
            ident=page.image_path, label=f'p. {page.label}', iiif=True)

        # Override default image quality and format set by prezi
        image.id = image.id.replace(f'/default.jpg', f'/{page.image_color_quality}.tif')
      
        image.format = page.format
        image.height = page.height
        image.width = page.width
        return annotation

    def add_search_service(self, manifest: Manifest, search_url: str):
        context = 'http://iiif.io/api/search/1/context.json'
        profile = 'http://iiif.io/api/search/1/search'
        
        manifest.add_service(ident=search_url, context=context, profile=profile)
