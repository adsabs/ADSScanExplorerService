"""Greyscale to Grayscale

Revision ID: 9a88382c235f
Revises: ee9fd2ef5b67
Create Date: 2022-09-01 15:39:11.645435

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9a88382c235f'
down_revision = 'ee9fd2ef5b67'
branch_labels = None
depends_on = None

old_options = ('BW', 'Greyscale', 'Color')
new_options = ('BW', 'Grayscale', 'Color')

old_type = sa.Enum(*old_options, name='pagecolor')
new_type = sa.Enum(*new_options, name='pagecolor')

def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
        # Create a tempoary "_status" type, convert and drop the "old" type
    op.execute('ALTER TABLE page ALTER COLUMN color_type TYPE varchar'
               ' USING color_type::text')
    old_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "new" status type
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute('UPDATE page SET color_type = REPLACE(color_type, \'Greyscale\', \'Grayscale\')')
    op.execute('ALTER TABLE page ALTER COLUMN color_type TYPE pagecolor'
               ' USING color_type::pagecolor')

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('ALTER TABLE page ALTER COLUMN color_type TYPE varchar'
               ' USING color_type::text')
    new_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "new" status type
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute('UPDATE page SET color_type = REPLACE(color_type, \'Grayscale\', \'Greyscale\')')
    op.execute('ALTER TABLE page ALTER COLUMN color_type TYPE pagecolor'
               ' USING color_type::pagecolor')
    
    # ### end Alembic commands ###
