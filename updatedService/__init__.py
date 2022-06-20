import logging, os, json, redis
from azure.cosmos import CosmosClient

import azure.functions as func


class Service:
    def __init__(self) -> None:
        pass
    
    def __init__(self, item) -> None:
        self.id = item['id']
        self.serviceNamespace = item['serviceNamespace']
        self.serviceOwner = item['serviceOwner']
        self.serviceVersion = item['serviceVersion']
        self.serviceReleaseDate = item['serviceReleaseDate']
        self.serviceRepositoryLink = item['serviceRepositoryLink']

class ServiceManager:
    def __init__(self) -> None:
        self.services = dict()
        
    def import_service(self, item):
        newService = Service(item)
        self.services[newService.id] = newService
        
    def load_from_db(self):
        logging.debug(f"Connecting to DB...")
        client = CosmosClient.from_connection_string(os.environ['wsoMainConnectionString'])
        db = client.get_database_client("service-repository")
        c = db.get_container_client("services")
        items = list(c.query_items(
            query="SELECT c.id, c.serviceNamespace, c.serviceOwner, c.serviceVersion, c.serviceReleaseDate, c.serviceRepositoryLink FROM c",
            enable_cross_partition_query=True
        ))
        for item in items:
            self.import_service(item)
    
    def count_services(self):
        return len(self.services)
    
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)
    
            
def main(documents: func.DocumentList) -> str:
    if documents:
        logging.info('Document id: %s', documents[0]['id'])
        logging.info(f"Rebuild completed.  Exported { rebuild_table_and_cache() } records.")

def rebuild_table_and_cache() -> int:
    sm = ServiceManager()
    sm.load_from_db()
    logging.debug(f"Connecting to redisHost { os.environ['redisHost'] }")
    r = redis.StrictRedis(host=os.environ['redisHost'], port=6380, password=os.environ['redisKey'], ssl=True)
    logging.info(f"Set for wso.webui.service.table returned { r.set('wso.webui.service.table', sm.toJSON()) }")
    logging.info(f"Set for wso.webui.service.count returned { r.set('wso.webui.service.count', sm.count_services()) }")
    return sm.count_services()
    

if __name__ == '__main__':
    print(f"Rebuild completed.  Exported { rebuild_table_and_cache() } records.")
