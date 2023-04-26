import pandas as pd
import requests
from datetime import datetime, timedelta
import mysql.connector


#Diccionario con las comunidades autónomas:
cod_comunidades = {'Ceuta': 8744,
                    'Melilla': 8745,
                    'Andalucía': 4,
                    'Aragón': 5,
                    'Cantabria': 6,
                    'Castilla - La Mancha': 7,
                    'Castilla y León': 8,
                    'Cataluña': 9,
                    'País Vasco': 10,
                    'Principado de Asturias': 11,
                    'Comunidad de Madrid': 13,
                    'Comunidad Foral de Navarra': 14,
                    'Comunitat Valenciana': 15,
                    'Extremadura': 16,
                    'Galicia': 17,
                    'Illes Balears': 8743,
                    'Canarias': 8742,
                    'Región de Murcia': 21,
                    'La Rioja': 20}




class Extraccion:
    
    def __init__(self, año_inicio, año_final):
        
        self.año_inicio = año_inicio
        self.año_final = año_final
        
    def nacional(self):  
        df_energia_nacional = pd.DataFrame()
        for año in range(self.año_inicio, self.año_final + 1):

            url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={año}-01-01T00:00&end_date={año}-12-31T23:59&time_trunc=year"
            response = requests.get(url=url)

            fichero_json_renovable = response.json()["included"][0]["attributes"]["values"] 
            fichero_json_no_renovable = response.json()["included"][1]["attributes"]["values"]

            df_renovables = pd.json_normalize(fichero_json_renovable)
            df_no_renovables = pd.json_normalize(fichero_json_no_renovable)

            df_renovables["tipo"] = response.json()["included"][0]["type"]
            df_no_renovables["tipo"] = response.json()["included"][1]["type"]
            
            df_energia_nacional = pd.concat([df_energia_nacional, df_renovables, df_no_renovables], axis = 0)
            
        return df_energia_nacional
    
    
    def ccaa(self, cod_comunidades):  
        
        self.cod_comunidades = cod_comunidades
        
        df_completo_ccaa = pd.DataFrame()
        for año in range(self.año_inicio, self.año_final + 1):

            for k, v in cod_comunidades.items():
                
                url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={año}-01-01T00:00&end_date={año}-12-31T23:59&time_trunc=year&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={v}"
            
                response2 = requests.get(url=url)

                if len(response2.json()["included"]) == 1:

                    fichero_json_no_renovable = response2.json()["included"][0]["attributes"]["values"] 
                    df_no_renovables = pd.json_normalize(fichero_json_no_renovable)
                    df_no_renovables["tipo"] = response2.json()["included"][0]["type"]
                    df_no_renovables["comunidad"] = k
                    df_no_renovables["id_comunidad"] = v  
                              
                    df_completo_ccaa = pd.concat([df_completo_ccaa, df_no_renovables], axis = 0)

                elif len(response2.json()["included"]) == 2:

                    fichero_json_no_renovable = response2.json()["included"][0]["attributes"]["values"] 
                    df_no_renovables = pd.json_normalize(fichero_json_no_renovable)
                    df_no_renovables["tipo"] = response2.json()["included"][0]["type"]
                    df_no_renovables["comunidad"] = k
                    df_no_renovables["id_comunidad"] = v           

                    fichero_json_renovable = response2.json()["included"][1]["attributes"]["values"] 
                    df_renovables = pd.json_normalize(fichero_json_renovable)
                    df_renovables["tipo"] = response2.json()["included"][1]["type"]
                    df_renovables["comunidad"] = k
                    df_renovables["id_comunidad"] = v   

                    df_completo_ccaa = pd.concat([df_completo_ccaa, df_no_renovables, df_renovables], axis = 0)


     
        return df_completo_ccaa
        
    def limpieza_censo(self, df):
        
        
        df.drop(["Comunidades_y_Ciudades_Autónomas"], axis =1, inplace = True)
        
        return df
        
    
    def limpieza_energias(self, df_energias):
        
        df_energias["value"]=df_energias["value"].round(2)
        df_energias["percentage"]=df_energias["percentage"].round(2)
        df_energias[["fecha","hora"]]=df_energias.datetime.str.split("T",expand = True)
        df_energias.drop(["hora"], axis = 1, inplace=True)
        df_energias["fecha"]=df_energias["fecha"].apply(pd.to_datetime)
        
        return df_energias
       



class CrearBBDD:

    def __init__(self, nombre_bbdd, contraseña):

        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña
    
    def creacion_bbdd(self):

        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}') 
        mycursor = mydb.cursor()
        print("Conexión realizada satisfactoriamente.")

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            
        except:
            print("La BBDD ya existe.")

    def creacion_insercion_tabla(self, query):
        
        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}', 
                                       database=f"{self.nombre_bbdd}") 
        mycursor = mydb.cursor()
        
        try:
            mycursor.execute(query)
            mydb.commit()
          
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)



#Variables con las queries de la creación de las tablas:

tabla_fechas = """
CREATE TABLE IF NOT EXISTS `energia`.`fechas` (
  `idfechas` INT NOT NULL,
  `fecha` DATE NULL,
  PRIMARY KEY (`idfechas`))
ENGINE = InnoDB;"""


tabla_comunidades = """
CREATE TABLE IF NOT EXISTS `energia`.`comunidades` (
  `idcomunidades` INT NOT NULL,
  `comunidad` VARCHAR(45) NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB; """


tabla_energia_nacional = """
CREATE TABLE IF NOT EXISTS `energia`.`nacional_renovable_no_renovable` (
  `idnacional_renovable_no_renovable` INT NOT NULL,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  PRIMARY KEY (`idnacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fechas_idx` (`fechas_idfechas` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fechas`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;"""


tabla_energia_comunidades = """
CREATE TABLE IF NOT EXISTS `energia`.`comunidades_renovables_no_renovables` (
  `idcomunidades_renovables_no_renovables` INT NOT NULL,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  `comunidades_idcomunidades` INT NOT NULL,
  PRIMARY KEY (`idcomunidades_renovables_no_renovables`),
  INDEX `fk_comunidades_renovables_no_renovables_fechas1_idx` (`fechas_idfechas` ASC) VISIBLE,
  INDEX `fk_comunidades_renovables_no_renovables_comunidades1_idx` (`comunidades_idcomunidades` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovables_no_renovables_fechas1`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_comunidades_renovables_no_renovables_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `energia`.`comunidades` (`idcomunidades`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
 """
