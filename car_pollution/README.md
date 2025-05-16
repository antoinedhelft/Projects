## Hi, 
🚗 We will study the polluting emissions of vehicles sold in France, using a dataset provided by Ademe.
The .csv file is available on this path :
https://www.data.gouv.fr/fr/datasets/r/3b3cce6b-073d-4b4c-a68c-2744c92f4045

(glossary : https://carlabelling.ademe.fr/index/glossaire)

The dataset provides informations of cars sold in France. There we find information concerning each vehicle allowing its identification : such as the mark, name of the model, the group to which the mark belongs, commercial description, fuel, body type, engine capacity, range, tax power, engine power, weight/power ratio, gearbox type, number of gear ratios.
Then we find informations regarding the minimum and maximum consumption at low, medium, high, very high and medium speeds.
And at the end, we find informations about emission at different speeds and emissions during a test drive (CO2, hydrocarbons, Nox, particles). The bonus-malus applied to the vehicle and the price of the vehicle.

🌡️ We will analyse the Global Warming Power (GWP) of each category of vehicule by grouping them by engine used ('Energy') and others polluants.

☁️ In this dataset we only have the pollutants emitted by the engine.
You should know that in these emissions we will have gases which will contribute to global warming, and others which will have an impact on the health of living beings.
The gases that have an effect on global warming are:
- water vapor
- CO2
- Hydrocarbons (HC)
- NO2
- Halogens
- O3

https://jancovici.com/changement-climatique/gaz-a-effet-de-serre-et-cycle-du-carbone/quels-sont-les-gaz-a-effet-de-serre-quels-sont-leurs-contribution-a-leffet-de-serre/

https://www.geo.fr/environnement/hydrocarbure-definition-classification-et-utilisation-193625

Among the pollutants impacting health, we will find:
- the Nox
- fine particles
- Volatile Organic Compounds

You should also know that pollutants can pass from one group to another by associating with other components present naturally or not.
https://www.ecologie.gouv.fr/pollution-lair-origines-situation-et-impacts