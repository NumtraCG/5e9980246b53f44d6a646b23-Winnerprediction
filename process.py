import json
import Connectors
import Transformations
import AutoML
try:
    Winnerprediction_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e9980246b53f44d6a646b24", spark, "{'url': '/Demo/Fifa.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib8073bbfa952efa9d363b234ce06e2c6', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    Winnerprediction_AutoFE = Transformations.TransformationMain.run(["5e9980246b53f44d6a646b24"], {"5e9980246b53f44d6a646b24": Winnerprediction_DBFS}, "5e9980246b53f44d6a646b25", spark, json.dumps({"FE": [{"transformationsData": {"feature_label": "Team"}, "feature": "Team", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "10", "mean": "", "stddev": "", "min": "Argentina", "max": "Uruguay", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "Goals_Scored", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "10", "mean": "6.7", "stddev": "2.41", "min": "3", "max": "12", "missing": "0"}}, {"transformationsData": {}, "feature": "Goals_Conceded", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "10", "mean": "4.7", "stddev": "2.41", "min": "1", "max": "9", "missing": "0"}}, {"transformationsData": {}, "feature": "Wins", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "10", "mean": "2.2", "stddev": "1.23", "min": "1", "max": "4", "missing": "0"}}, {"transformationsData": {}, "feature": "Attempts", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "10", "mean": "55.0", "stddev": "11.67", "min": "39", "max": "77", "missing": "0"}}, {
                                                                     "transformationsData": {}, "feature": "Attemps_On_Target", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "10", "mean": "17.7", "stddev": "5.1", "min": "12", "max": "30", "missing": "0"}}, {"transformationsData": {}, "feature": "Pass_Accuracy", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "10", "mean": "0.85", "stddev": "0.04", "min": "0.77", "max": "0.91", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Distance_Covered", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "10", "mean": "263.2", "stddev": "12.04", "min": "245", "max": "283", "missing": "0"}}, {"transformationsData": {"feature_label": "W_L"}, "feature": "W_L", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "10", "mean": "", "stddev": "", "min": "L", "max": "W", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Team_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "10", "mean": "4.5", "stddev": "3.03", "min": "0.0", "max": "9.0", "missing": "0"}}, {"feature": "W_L_transform", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "10", "mean": "0.4", "stddev": "0.52", "min": "0", "max": "1", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionClassification(Winnerprediction_AutoFE, [
                                  "Team", "Goals_Scored", "Goals_Conceded", "Wins", "Attempts", "Attemps_On_Target", "Pass_Accuracy", "Distance_Covered"], "W_L")

except Exception as ex:
    print(ex)
