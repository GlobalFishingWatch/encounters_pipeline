import datetime
import pytz
from .test_resample import ResampledRecord

path_1 = [
    ResampledRecord("2011-01-01 15:40:00 UTC", -1.5032387, 55.2340155, id='1'),
    ResampledRecord("2011-01-01 15:50:00 UTC", -1.4869308, 55.232743, id='1'),
    ResampledRecord("2011-01-01 16:00:00 UTC", -1.4752579, 55.2292189, id='1'),
    # Encounter start.
    ResampledRecord("2011-01-01 16:10:00 UTC", -1.4719963, 55.2251069, id='1'),
    ResampledRecord("2011-01-01 16:20:00 UTC", -1.471653, 55.2224633, id='1'),
    ResampledRecord("2011-01-01 16:30:00 UTC", -1.4718246, 55.21991750, id='1'),
    ResampledRecord("2011-01-01 16:40:00 UTC", -1.472168, 55.2185466, id='1'),
    ResampledRecord("2011-01-01 16:50:00 UTC", -1.4718246, 55.2167839, id='1'),
    ResampledRecord("2011-01-01 17:00:00 UTC", -1.4725113, 55.2156088, id='1'),
    ResampledRecord("2011-01-01 17:10:00 UTC", -1.4730263, 55.2139439, id='1'),
    # Encounter end.
    ResampledRecord("2011-01-01 17:20:00 UTC", -1.4859009, 55.2089489, id='1'),
    ResampledRecord("2011-01-01 17:30:00 UTC", -1.4974022, 55.2078715, id='1'),
    ResampledRecord("2011-01-01 17:40:00 UTC", -1.5140533, 55.2069899, id='1')
]

path_2 = [
    ResampledRecord("2011-01-01 15:20:00 UTC", -1.4065933, 55.2350923, id='2'),
    ResampledRecord("2011-01-01 15:30:00 UTC", -1.4218712, 55.2342113, id='2'),
    ResampledRecord("2011-01-01 15:40:00 UTC", -1.4467621, 55.2334282, id='2'),
    ResampledRecord("2011-01-01 15:50:00 UTC", -1.4623833, 55.2310789, id='2'),
    ResampledRecord("2011-01-01 16:00:00 UTC", -1.469593, 55.2287294, id='2'),
    # Encounter start.
    ResampledRecord("2011-01-01 16:10:00 UTC", -1.471138, 55.2267713, id='2'),
    ResampledRecord("2011-01-01 16:30:00 UTC", -1.4704514, 55.2206029, id='2'),
    ResampledRecord("2011-01-01 16:40:00 UTC", -1.4704514, 55.218057, id='2'),
    ResampledRecord("2011-01-01 16:50:00 UTC", -1.4704514, 55.215217, id='2'),
    ResampledRecord("2011-01-01 17:00:00 UTC", -1.4728546, 55.2116913, id='2'),
    # Encounter end.
    ResampledRecord("2011-01-01 17:10:00 UTC", -1.4718246, 55.2088509, id='2'),
    ResampledRecord("2011-01-01 17:20:00 UTC", -1.4474487, 55.2057165, id='2'),
    ResampledRecord("2011-01-01 17:30:00 UTC", -1.4278793, 55.2040512, id='2'),
    ResampledRecord("2011-01-01 17:40:00 UTC", -1.4084816, 55.2036594, id='2'),
    ResampledRecord("2011-01-01 17:50:00 UTC", -1.3998985, 55.2037574, id='2')
]

simple_series_data = path_1 + path_2


dl_path_1 = [
    ResampledRecord("2011-01-01 15:40:00 UTC", -1.5032387, 179.999, id='1'),
    ResampledRecord("2011-01-01 15:50:00 UTC", -1.4869308, 179.999, id='1'),
    ResampledRecord("2011-01-01 16:00:00 UTC", -1.4752579, 179.999, id='1'),
    # Encounter start.
    ResampledRecord("2011-01-01 16:10:00 UTC", -1.4719963, 179.999, id='1'),
    ResampledRecord("2011-01-01 16:20:00 UTC", -1.471653, 179.999, id='1'),
    ResampledRecord("2011-01-01 16:30:00 UTC", -1.4718246, 179.999, id='1'),
    ResampledRecord("2011-01-01 16:40:00 UTC", -1.472168, 179.999, id='1'),
    ResampledRecord("2011-01-01 16:50:00 UTC", -1.4718246, 179.999, id='1'),
    ResampledRecord("2011-01-01 17:00:00 UTC", -1.4725113, 179.999, id='1'),
    ResampledRecord("2011-01-01 17:10:00 UTC", -1.4730263, 179.999, id='1'),
    # Encounter end.
    ResampledRecord("2011-01-01 17:20:00 UTC", -1.4859009, 179.999, id='1'),
    ResampledRecord("2011-01-01 17:30:00 UTC", -1.4974022, 179.999, id='1'),
    ResampledRecord("2011-01-01 17:40:00 UTC", -1.5140533, 179.999, id='1')
]

dl_path_2 = [
    ResampledRecord("2011-01-01 15:20:00 UTC", -1.4065933, -179.9993, id='2'),
    ResampledRecord("2011-01-01 15:30:00 UTC", -1.4218712, -179.9993, id='2'),
    ResampledRecord("2011-01-01 15:40:00 UTC", -1.4467621, -179.9992, id='2'),
    ResampledRecord("2011-01-01 15:50:00 UTC", -1.4623833, -179.9999, id='2'),
    ResampledRecord("2011-01-01 16:00:00 UTC", -1.469593,  -179.9994, id='2'),
    # Encounter start.
    ResampledRecord("2011-01-01 16:10:00 UTC", -1.471138,  -179.9993, id='2'),
    ResampledRecord("2011-01-01 16:30:00 UTC", -1.4704514, -179.9999, id='2'),
    ResampledRecord("2011-01-01 16:40:00 UTC", -1.4704514, -179.999, id='2'),
    ResampledRecord("2011-01-01 16:50:00 UTC", -1.4704514, -179.999, id='2'),
    ResampledRecord("2011-01-01 17:00:00 UTC", -1.4728546, -179.9993, id='2'),
    # Encounter end.
    ResampledRecord("2011-01-01 17:10:00 UTC", -1.4718246, -179.999, id='2'),
    ResampledRecord("2011-01-01 17:20:00 UTC", -1.4474487, -179.995, id='2'),
    ResampledRecord("2011-01-01 17:30:00 UTC", -1.4278793, -179.992, id='2'),
    ResampledRecord("2011-01-01 17:40:00 UTC", -1.4084816, -179.994, id='2'),
    ResampledRecord("2011-01-01 17:50:00 UTC", -1.3998985, -179.994, id='2')
]

dateline_series_data = dl_path_1 + dl_path_2


def text_to_records(vessel_id, text):

    def line_to_record(x):
        seconds, lat, lon, _ = x.split(',')
        timestamp = datetime.datetime.utcfromtimestamp(float(seconds)).replace(tzinfo=pytz.utc)
        return ResampledRecord(timestamp, float(lat), float(lon), id=vessel_id)

    return [line_to_record(x) for x in text.split('\n')]

mmsi_1 = 441910000
series1 = text_to_records(str(mmsi_1), """1.426741942E9,-27.4298992157,38.5174331665,514311.21875
1.426743579E9,-27.4326496124,38.5220680237,514311.21875
1.426750357E9,-27.4843673706,38.535533905,519884.34375
1.426750387E9,-27.4843826294,38.5355834961,519884.34375
1.426750396E9,-27.4843826294,38.5356178284,519884.34375
1.426750397E9,-27.4843826294,38.5356178284,519884.34375
1.426750407E9,-27.4843997955,38.5356178284,519884.34375
1.426750476E9,-27.4844169617,38.5357666016,519884.34375
1.426750477E9,-27.4844169617,38.5357666016,519884.34375
1.426750487E9,-27.4844169617,38.5358009338,519884.34375
1.426750488E9,-27.4844169617,38.5358009338,519884.34375
1.426750498E9,-27.4844169617,38.5358161926,519884.34375
1.426750547E9,-27.4844493866,38.5359153748,519884.34375
1.426750548E9,-27.4844493866,38.5359153748,519884.34375
1.426750566E9,-27.4844493866,38.535949707,519884.34375
1.426750567E9,-27.4844493866,38.535949707,519884.34375
1.426750607E9,-27.4844837189,38.5359992981,519884.34375
1.426750608E9,-27.4844837189,38.5359992981,519884.34375
1.426750626E9,-27.4845161438,38.5360336304,519884.34375
1.426750627E9,-27.4845161438,38.5360336304,519884.34375
1.426750646E9,-27.4845161438,38.5360679626,519884.34375
1.426750668E9,-27.4845333099,38.5360984802,519884.34375
1.426750686E9,-27.4845504761,38.5361175537,519884.34375
1.426750687E9,-27.4845504761,38.5361175537,519884.34375
1.426750738E9,-27.4845504761,38.5361824036,519884.34375
1.426750746E9,-27.4845657349,38.5361824036,519884.34375
1.426750756E9,-27.4845657349,38.5362167358,519884.34375
1.426750757E9,-27.4845657349,38.5362167358,519884.34375
1.426750806E9,-27.484582901,38.5362510681,520609.90625
1.426750807E9,-27.484582901,38.5362510681,520609.90625
1.426750857E9,-27.4845657349,38.5362815857,520609.90625
1.426750936E9,-27.4845161438,38.5363502502,520609.90625
1.426750937E9,-27.4845161438,38.5363502502,520609.90625
1.426751026E9,-27.4844493866,38.5364151001,520609.90625
1.426751027E9,-27.4844493866,38.5364151001,520609.90625
1.426751037E9,-27.4844493866,38.5364151001,520609.90625
1.426751076E9,-27.4844169617,38.5364494324,520609.90625
1.426751078E9,-27.4844169617,38.5364494324,520609.90625
1.426751138E9,-27.4843673706,38.5364990234,520609.90625
1.426754589E9,-27.4824333191,38.5363998413,520609.90625
1.426756277E9,-27.480550766,38.5360183716,519884.34375
1.426756356E9,-27.4804496765,38.535949707,519884.34375
1.426756357E9,-27.4804496765,38.535949707,519884.34375
1.426756367E9,-27.4804325104,38.5359344482,519884.34375
1.426756368E9,-27.4804325104,38.5359344482,519884.34375
1.426756376E9,-27.4804172516,38.5359153748,519884.34375
1.426756377E9,-27.4804172516,38.5359153748,519884.34375
1.426756397E9,-27.4803829193,38.535900116,519884.34375
1.426756446E9,-27.4803333282,38.5358505249,519884.34375
1.426756476E9,-27.4803009033,38.5358161926,519884.34375
1.426756477E9,-27.4803009033,38.5358161926,519884.34375
1.426756497E9,-27.4802837372,38.5358009338,519884.34375
1.426756537E9,-27.4802341461,38.5357666016,519884.34375
1.426756538E9,-27.4802341461,38.5357666016,519884.34375
1.426756557E9,-27.4801826477,38.5357666016,519884.34375
1.426756597E9,-27.4801158905,38.5357170105,519884.34375
1.426756598E9,-27.4801158905,38.5357170105,519884.34375
1.426756657E9,-27.4799995422,38.5356674194,519884.34375
1.426756658E9,-27.4799995422,38.5356674194,519884.34375
1.426756678E9,-27.4799671173,38.5356483459,519884.34375
1.426756747E9,-27.479850769,38.5355834961,519884.34375
1.426756836E9,-27.4797496796,38.5355148315,519884.34375
1.426760374E9,-27.4754505157,38.5312156677,519196.25
1.426789665E9,-27.4620990753,38.5190658569,517056.96875
1.426789666E9,-27.4620990753,38.5190658569,517056.96875
1.426789677E9,-27.4621162415,38.5190658569,517056.96875
1.426789697E9,-27.4621162415,38.5190658569,517056.96875
1.426789725E9,-27.4621334076,38.5190658569,517056.96875
1.426789726E9,-27.4621334076,38.5190658569,517056.96875
1.426789737E9,-27.4621334076,38.5190658569,517056.96875
1.426789796E9,-27.4621658325,38.5190658569,517056.96875
1.426789797E9,-27.4621658325,38.5190658569,517056.96875
1.426789856E9,-27.4622001648,38.5190505981,517056.96875
1.426789857E9,-27.4622001648,38.5190505981,517056.96875
1.426789915E9,-27.4622325897,38.5190505981,517056.96875
1.426789945E9,-27.4622497559,38.5190162659,517056.96875
1.426789946E9,-27.4622497559,38.5190162659,517056.96875
1.426789974E9,-27.4622497559,38.5190162659,517056.96875
1.426789975E9,-27.4622497559,38.5190162659,517056.96875
1.426790005E9,-27.462266922,38.5190010071,517056.96875
1.426790006E9,-27.462266922,38.5190010071,517056.96875
1.426790015E9,-27.462266922,38.5189819336,517056.96875
1.426790034E9,-27.4622840881,38.5189666748,517056.96875
1.426790035E9,-27.4622840881,38.5189666748,517056.96875
1.426790076E9,-27.4622840881,38.518951416,517056.96875
1.426790215E9,-27.4622993469,38.5189170837,517056.96875
1.426790307E9,-27.4623165131,38.518901825,517056.96875
1.426790367E9,-27.4623336792,38.5188674927,517056.96875
1.426795576E9,-27.4648666382,38.5223350525,518470.625
1.426795585E9,-27.4648838043,38.5223503113,518470.625
1.426795586E9,-27.4648838043,38.5223503113,518470.625
1.426795596E9,-27.4648838043,38.5223655701,518470.625
1.426795596E9,-27.4648838043,38.5223655701,518470.625
1.426795656E9,-27.4648990631,38.5224494934,518470.625
1.426795656E9,-27.4648990631,38.5224494934,518470.625
1.426795705E9,-27.4649162292,38.5224685669,518470.625
1.426795706E9,-27.4649162292,38.5224685669,518470.625
1.426795716E9,-27.4649333954,38.5224838257,518470.625
1.426795716E9,-27.4649333954,38.5224838257,518470.625
1.426795726E9,-27.4649333954,38.5224990845,518470.625
1.426795765E9,-27.4649333954,38.5225334167,518470.625
1.426795766E9,-27.4649333954,38.5225334167,518470.625
1.426795776E9,-27.4649333954,38.5225486755,518470.625
1.426795776E9,-27.4649333954,38.5225486755,518470.625
1.426795794E9,-27.4649333954,38.522567749,518470.625
1.426795795E9,-27.4649333954,38.522567749,518470.625
1.426795806E9,-27.4649333954,38.5225830078,518470.625
1.426795806E9,-27.4649333954,38.5225830078,518470.625
1.426795816E9,-27.4649505615,38.5225830078,518470.625
1.426795816E9,-27.4649505615,38.5225830078,518470.625
1.426795854E9,-27.4649658203,38.5226325989,518470.625
1.426795855E9,-27.4649658203,38.5226325989,518470.625
1.426795876E9,-27.4650001526,38.5226516724,518470.625
1.426795876E9,-27.4650001526,38.5226516724,518470.625
1.426795896E9,-27.4650001526,38.5226516724,518470.625
1.426795935E9,-27.4650325775,38.5226821899,518470.625
1.426795946E9,-27.4650325775,38.5227012634,518470.625
1.426795946E9,-27.4650325775,38.5227012634,518470.625
1.426795954E9,-27.4650325775,38.5227012634,518470.625
1.426795995E9,-27.4650497437,38.5227165222,518470.625
1.426795996E9,-27.4650497437,38.5227165222,518470.625
1.426796015E9,-27.4650497437,38.5227165222,518470.625
1.426796055E9,-27.4650497437,38.5227508545,518470.625
1.426796056E9,-27.4650497437,38.5227508545,518470.625
1.426796066E9,-27.4650497437,38.5227508545,518470.625
1.426796066E9,-27.4650497437,38.5227508545,518470.625
1.426796134E9,-27.4650669098,38.5228004456,518470.625
1.426796135E9,-27.4650669098,38.5228004456,518470.625
1.426796194E9,-27.4650840759,38.5228004456,518470.625
1.426796195E9,-27.4650840759,38.5228004456,518470.625
1.426801056E9,-27.4607830048,38.5253982544,517782.65625""")

mmsi_2 = 563418000
series2 = text_to_records(str(mmsi_2), """1.426723864E9,-27.4495792389,38.6691398621,528172.5625
1.426724343E9,-27.4489612579,38.6695671082,528172.5625
1.426725104E9,-27.4481124878,38.6698951721,528172.5625
1.426725195E9,-27.4479999542,38.6697616577,528172.5625
1.426725533E9,-27.4476013184,38.6692085266,528172.5625
1.426725544E9,-27.4475955963,38.6691932678,528172.5625
1.426726264E9,-27.4470424652,38.6678962708,528172.5625
1.426726761E9,-27.4434890747,38.6676521301,527502.9375
1.426727555E9,-27.4591960907,38.6337394714,526560.9375
1.426727555E9,-27.4591960907,38.6337394714,526560.9375
1.426728085E9,-27.4703998566,38.6073608398,525767.5
1.426729023E9,-27.4843769073,38.5645599365,522792.09375
1.426730323E9,-27.4863567352,38.5427284241,521298.0625
1.426730684E9,-27.4854450226,38.5430717468,520609.90625
1.426731143E9,-27.4847755432,38.5437278748,521336.375
1.426731154E9,-27.4847679138,38.5437393188,521336.375
1.426731405E9,-27.4845657349,38.5437049866,521336.375
1.426731454E9,-27.4845294952,38.5436897278,521336.375
1.426731465E9,-27.4845256805,38.5436859131,521336.375
1.426731904E9,-27.4843425751,38.5433197021,521336.375
1.426732613E9,-27.4842453003,38.5422744751,520609.90625
1.426733133E9,-27.483997345,38.5421638489,520609.90625
1.426733913E9,-27.4836807251,38.542339325,520609.90625
1.426734504E9,-27.4835643768,38.5425415039,520609.90625
1.426735194E9,-27.4833507538,38.5435447693,521336.375
1.426735913E9,-27.4820175171,38.5440483093,521336.375
1.426736454E9,-27.4810028076,38.5427894592,520609.90625
1.426737143E9,-27.4803276062,38.5413551331,520609.90625
1.426737253E9,-27.4802913666,38.5412139893,520609.90625
1.426737624E9,-27.480266571,38.5408782959,520609.90625
1.426737675E9,-27.4802703857,38.5408477783,520609.90625
1.426738384E9,-27.4803848267,38.5402908325,520609.90625
1.426738853E9,-27.4803009033,38.5400009155,520609.90625
1.426739643E9,-27.4809513092,38.5406799316,520609.90625
1.426740033E9,-27.4814605713,38.5411453247,520609.90625
1.426741663E9,-27.4837875366,38.5415611267,520609.90625
1.426742624E9,-27.4847946167,38.5421524048,520609.90625
1.426743485E9,-27.4850273132,38.5418548584,520609.90625
1.426743854E9,-27.485162735,38.5416946411,520609.90625
1.426744013E9,-27.4852104187,38.5416488647,520609.90625
1.426744781E9,-27.4852600098,38.5413742065,520609.90625
1.426744781E9,-27.4852600098,38.5413742065,520609.90625
1.426744802E9,-27.4852600098,38.5413475037,520609.90625
1.426744803E9,-27.4852600098,38.5413475037,520609.90625
1.426744813E9,-27.4852561951,38.5413322449,520609.90625
1.426744813E9,-27.4852561951,38.5413322449,520609.90625
1.426744863E9,-27.4852352142,38.5412750244,520609.90625
1.426744893E9,-27.4852142334,38.5412406921,520609.90625
1.426744901E9,-27.4852085114,38.5412254333,520609.90625
1.426744902E9,-27.4852085114,38.5412254333,520609.90625
1.426745293E9,-27.4849643707,38.540599823,520609.90625
1.426746313E9,-27.4841480255,38.5397224426,520609.90625
1.426747743E9,-27.4827594757,38.5382804871,520609.90625
1.426749253E9,-27.4840908051,38.5353813171,519884.34375
1.426750362E9,-27.4842319489,38.5357818604,519884.34375
1.426750371E9,-27.4842357635,38.5358009338,519884.34375
1.426750401E9,-27.4842472076,38.5358543396,519884.34375
1.426750402E9,-27.4842472076,38.5358543396,519884.34375
1.426750412E9,-27.484249115,38.5358772278,519884.34375
1.426750413E9,-27.484249115,38.5358772278,519884.34375
1.426750431E9,-27.4842567444,38.5359115601,519884.34375
1.426750461E9,-27.4842758179,38.5359802246,519884.34375
1.426750462E9,-27.4842758179,38.5359802246,519884.34375
1.426750472E9,-27.484287262,38.5360031128,519884.34375
1.426750473E9,-27.484287262,38.5360031128,519884.34375
1.426750491E9,-27.4842987061,38.5360412598,519884.34375
1.426750492E9,-27.4842987061,38.5360412598,519884.34375
1.426750521E9,-27.4843292236,38.5361061096,519884.34375
1.426750522E9,-27.4843292236,38.5361061096,519884.34375
1.426750601E9,-27.4843788147,38.5362472534,520609.90625
1.426750602E9,-27.4843788147,38.5362472534,520609.90625
1.426750632E9,-27.4843978882,38.5362930298,520609.90625
1.426750633E9,-27.4843978882,38.5362930298,520609.90625
1.426750641E9,-27.4844074249,38.5363082886,520609.90625
1.426750642E9,-27.4844074249,38.5363082886,520609.90625
1.426750683E9,-27.4844436646,38.536365509,520609.90625
1.426750685E9,-27.4844436646,38.536365509,520609.90625
1.426750702E9,-27.4844608307,38.5363922119,520609.90625
1.426750702E9,-27.4844608307,38.5363922119,520609.90625
1.426750713E9,-27.4844722748,38.5364151001,520609.90625
1.426750721E9,-27.4844760895,38.5364303589,520609.90625
1.426750761E9,-27.484500885,38.53647995,520609.90625
1.426750762E9,-27.484500885,38.53647995,520609.90625
1.426750772E9,-27.4845104218,38.536491394,520609.90625
1.426750821E9,-27.4845237732,38.5365409851,520609.90625
1.426750822E9,-27.4845237732,38.5365409851,520609.90625
1.426750832E9,-27.4845256805,38.5365486145,520609.90625
1.426750833E9,-27.4845256805,38.5365486145,520609.90625
1.426750871E9,-27.4845256805,38.5365791321,520609.90625
1.426750872E9,-27.4845256805,38.5365791321,520609.90625
1.426750901E9,-27.4845161438,38.536605835,520609.90625
1.426750902E9,-27.4845161438,38.536605835,520609.90625
1.426750932E9,-27.484500885,38.5366325378,520609.90625
1.426751103E9,-27.4843654633,38.5367546082,520609.90625
1.426751142E9,-27.484336853,38.536781311,520609.90625
1.426751153E9,-27.4843273163,38.5367851257,520609.90625
1.426754503E9,-27.482465744,38.5366897583,520609.90625
1.426754538E9,-27.4816665649,38.5366668701,520609.90625
1.426754718E9,-27.4816665649,38.5366668701,520609.90625
1.42675628E9,-27.480474472,38.536315918,520609.90625
1.426756281E9,-27.480474472,38.536315918,520609.90625
1.42675638E9,-27.4803752899,38.5362205505,519884.34375
1.426756381E9,-27.4803752899,38.5362205505,519884.34375
1.4267565E9,-27.4802398682,38.536113739,519884.34375
1.426756501E9,-27.4802398682,38.536113739,519884.34375
1.42675656E9,-27.4801521301,38.5360565186,519884.34375
1.426756561E9,-27.4801521301,38.5360565186,519884.34375
1.426756721E9,-27.4798374176,38.5359115601,519884.34375
1.426756722E9,-27.4798374176,38.5359115601,519884.34375
1.426756782E9,-27.4797458649,38.5358505249,519884.34375
1.426760123E9,-27.4750003815,38.531665802,519196.25
1.426760264E9,-27.4754486084,38.5316238403,519196.25
1.426788613E9,-27.461687088,38.519329071,517056.96875
1.426789553E9,-27.4619560242,38.5193252563,517056.96875
1.426789714E9,-27.4620132446,38.5193138123,517056.96875
1.426789715E9,-27.4620132446,38.5193138123,517056.96875
1.426789792E9,-27.4620552063,38.5193099976,517056.96875
1.426789793E9,-27.4620552063,38.5193099976,517056.96875
1.426789803E9,-27.4620609283,38.5193061829,517056.96875
1.426789843E9,-27.4620742798,38.5193061829,517056.96875
1.426789843E9,-27.4620742798,38.5193061829,517056.96875
1.426789862E9,-27.4620857239,38.5193023682,517056.96875
1.426789863E9,-27.4620857239,38.5193023682,517056.96875
1.426789903E9,-27.4621067047,38.5192947388,517056.96875
1.426789903E9,-27.4621067047,38.5192947388,517056.96875
1.426789922E9,-27.4621143341,38.5192871094,517056.96875
1.426789932E9,-27.4621238708,38.5192871094,517056.96875
1.426789954E9,-27.4621391296,38.5192718506,517056.96875
1.426789955E9,-27.4621391296,38.5192718506,517056.96875
1.426789962E9,-27.4621448517,38.5192718506,517056.96875
1.426789963E9,-27.4621448517,38.5192718506,517056.96875
1.426789982E9,-27.4621601105,38.5192604065,517056.96875
1.426789983E9,-27.4621601105,38.5192604065,517056.96875
1.426790012E9,-27.462184906,38.5192451477,517056.96875
1.426790013E9,-27.462184906,38.5192451477,517056.96875
1.426790033E9,-27.4622001648,38.519241333,517056.96875
1.426790065E9,-27.4622135162,38.5192184448,517056.96875
1.426790072E9,-27.4622154236,38.5192184448,517056.96875
1.426790073E9,-27.4622154236,38.5192184448,517056.96875
1.426790192E9,-27.4622516632,38.5191726685,517056.96875
1.426790193E9,-27.4622516632,38.5191726685,517056.96875
1.426790243E9,-27.4622592926,38.5191612244,517056.96875
1.426790244E9,-27.4622592926,38.5191612244,517056.96875
1.426790252E9,-27.4622592926,38.5191612244,517056.96875
1.426790262E9,-27.4622612,38.5191612244,517056.96875
1.426790263E9,-27.4622612,38.5191612244,517056.96875
1.426790282E9,-27.4622612,38.519153595,517056.96875
1.426790312E9,-27.4622650146,38.5191459656,517056.96875
1.426790313E9,-27.4622650146,38.5191459656,517056.96875
1.426790325E9,-27.4622688293,38.5191421509,517056.96875
1.426790334E9,-27.462272644,38.5191383362,517056.96875
1.426790384E9,-27.4622879028,38.5191268921,517056.96875
1.426790393E9,-27.4622898102,38.5191268921,517056.96875
1.426790394E9,-27.4622898102,38.5191268921,517056.96875
1.426790652E9,-27.4623241425,38.5190811157,517056.96875
1.426790754E9,-27.4623775482,38.5190658569,517056.96875
1.426791103E9,-27.4626350403,38.5192146301,517056.96875
1.426791784E9,-27.4629135132,38.5195541382,517056.96875
1.426791844E9,-27.4629669189,38.5195884705,517056.96875
1.426792125E9,-27.4632492065,38.5197372437,517056.96875
1.426792424E9,-27.4634685516,38.5198516846,517056.96875
1.426793194E9,-27.4639568329,38.5203475952,517056.96875
1.426793713E9,-27.4638595581,38.5207824707,517056.96875
1.426794202E9,-27.4639091492,38.5212593079,517056.96875
1.426794223E9,-27.463924408,38.5212783813,517056.96875
1.426794524E9,-27.4641189575,38.5215530396,517056.96875
1.426795443E9,-27.4647579193,38.5224838257,518470.625
1.426795453E9,-27.4647655487,38.5224914551,518470.625
1.426795503E9,-27.4647941589,38.5225410461,518470.625
1.426795554E9,-27.4648189545,38.5225830078,518470.625
1.426795693E9,-27.4648990631,38.5227546692,518470.625
1.426795693E9,-27.4648990631,38.5227546692,518470.625
1.426795715E9,-27.4649047852,38.5227737427,518470.625
1.426795742E9,-27.4649124146,38.5228157043,518470.625
1.426795743E9,-27.4649124146,38.5228157043,518470.625
1.426795758E9,-27.4633331299,38.5216674805,517056.96875
1.426795802E9,-27.4649200439,38.5228691101,518470.625
1.426795803E9,-27.4649200439,38.5228691101,518470.625
1.426795863E9,-27.4649486542,38.5229187012,518470.625
1.426795873E9,-27.4649524689,38.5229263306,518470.625
1.426795874E9,-27.4649524689,38.5229263306,518470.625
1.426795904E9,-27.4649715424,38.5229492188,518470.625
1.426795904E9,-27.4649715424,38.5229492188,518470.625
1.426795933E9,-27.4649887085,38.5229797363,518470.625
1.426795939E9,-27.4633331299,38.5216674805,517056.96875
1.426795942E9,-27.4649944305,38.5229873657,518470.625
1.426795943E9,-27.4649944305,38.5229873657,518470.625
1.426795993E9,-27.4650096893,38.523021698,518470.625
1.426795994E9,-27.4650096893,38.523021698,518470.625
1.426796002E9,-27.4650096893,38.523021698,518470.625
1.426796144E9,-27.4650325775,38.5230789185,518470.625
1.426796194E9,-27.4650363922,38.5231018066,518470.625
1.426797353E9,-27.46534729,38.5238113403,518470.625
1.426797905E9,-27.4649085999,38.5240325928,518470.625
1.426800384E9,-27.4616909027,38.5251045227,517782.65625
1.426800564E9,-27.4614143372,38.5252151489,517782.65625
1.426800564E9,-27.4614143372,38.5252151489,517782.65625
1.426800964E9,-27.4609584808,38.5255470276,517782.65625
1.426801064E9,-27.4608802795,38.5256576538,517782.65625
1.426801159E9,-27.4599990845,38.5250015259,517782.65625
1.426801339E9,-27.4599990845,38.5250015259,517782.65625
1.426801519E9,-27.4599990845,38.5250015259,517782.65625
1.426801673E9,-27.4603004456,38.5263290405,517782.65625
1.426801913E9,-27.4599914551,38.5264472961,517782.65625
1.426802944E9,-27.458612442,38.526550293,517782.65625
1.426804324E9,-27.4555225372,38.5259513855,517095.59375
1.426804924E9,-27.4542350769,38.5253791809,517095.59375
1.426805953E9,-27.452419281,38.5248985291,517095.59375
1.426805953E9,-27.452419281,38.5248985291,517095.59375
1.426806214E9,-27.4520359039,38.5249061584,517095.59375
1.426806754E9,-27.4511795044,38.5250015259,517095.59375
1.426806905E9,-27.4508743286,38.5249938965,517095.59375
1.426807145E9,-27.4504871368,38.5248718262,516409.625
1.426808115E9,-27.4486637115,38.5238609314,516409.625
1.426809304E9,-27.4456787109,38.5228614807,516409.625""")

real_series_data = series1 + series2
