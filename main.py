from controller.pipeline import run

DATASET = "HubSpot"
TABLE = "Deals"


def main(request) -> dict:
    request_json = request.get_json()
    print(request_json)

    response = run(DATASET, TABLE, request_json)
    print(response)
    return response
