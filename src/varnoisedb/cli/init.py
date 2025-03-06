import click
from varnoisedb.database import DatabaseAdapter

@click.command()
@click.pass_context
def init(ctx):
    """Initialize the database by creating the necessary tables."""
    config = ctx.obj['CONFIG']
    db_config = config['database']
    
    db_adapter = DatabaseAdapter(
        db_type=db_config['type'],
        db_name=db_config['name'],
        host=db_config.get('host'),
        port=db_config.get('port'),
        user=db_config.get('user'),
        password=db_config.get('password')
    )
    
    db_adapter.create_tables()
    db_adapter.create_indices()
    
    db_adapter.close()
    click.echo("Database initialized successfully.")
