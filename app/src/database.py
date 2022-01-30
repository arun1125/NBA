import boto3

dynamoDB = boto3.resource(
    'dynamodb', 
    region_name = 'us-east-2'
    )
game_log_db = dynamoDB.Table('game_log')
historical_pbp_modelled_db = dynamoDB.Table('historical_pbp_modelled')

