################################################################################
# Module Name: advGPUStats.py                                                  #
#                                                                              #
# Copyright (c) 2020, IBM Corporation.  All rights reserved.                   #
#                                                                              #
# Redistribution and use in source and binary forms, with or without           #
# modification, are not permitted.                                             #
################################################################################

from pynvml import *
from datetime import datetime
from kafka import KafkaProducer
from enum import Enum
from kafka.errors import KafkaError
import socket
import json
import sys
import logging

########################
#  ENUMS
########################

#Enum to represent NVML Memory Locations
class Memory_Location(Enum):

     NVML_MEMORY_LOCATION_L1_CACHE = 0
     NVML_MEMORY_LOCATION_L2_CACHE = 1
     NVML_MEMORY_LOCATION_DRAM = 2
     NVML_MEMORY_LOCATION_REGISTER_FILE = 3
     NVML_MEMORY_LOCATION_TEXTURE_MEMORY =4
     NVML_MEMORY_LOCATION_TEXTURE_SHM = 5
     NVML_MEMORY_LOCATION_CBU = 6
     NVML_MEMORY_LOCATION_SRAM = 7
     NVML_MEMORY_LOCATION_COUNT = 8

#Enum to represent NVML Counter Types
class Memory_Counter_Type(Enum):

     NVML_VOLATILE_ECC = 0
     NVML_AGGREGATE_ECC = 1
     NVML_ECC_COUNTER_TYPE_COUNT = 2

########################
#  Global Variables
########################

#Variables used for ECC Errors
dbErrorType = NVML_MEMORY_ERROR_TYPE_UNCORRECTED
sbErrorType = NVML_MEMORY_ERROR_TYPE_CORRECTED

########################
#  Helper Functions
########################


def OMRFLogger(name, omrfLogFile):

     #Create Logger for OMRF
     logger = logging.getLogger(name)
     logger.setLevel(logging.DEBUG)

     #Create file handler and set Level to DEBUG
     fh = logging.FileHandler(omrfLogFile)
     fh.setLevel(logging.DEBUG)

     #Formate the Date/Time and add the File Formatter to the FH, and add the FH to the logger
     ff = logging.Formatter('%(levelname)s - %(asctime)s:%(msecs)03d  - %(name)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
     fh.setFormatter(ff)
     logger.addHandler(fh)

     return logger

#Format Current Time to "%Y-%m-%d %H:%M:%S"
def GetFormattedCurrentTime():
     now = datetime.utcnow()
     currentTime = now.strftime("%Y-%m-%d %H:%M:%S")
     return currentTime

#Acquire Producer
def GetProducer(logger):
     try:
          producerName = sys.argv[1] + ":9092"
     except IndexError:
          logger.error('Command Line Error: No Producer Hostname Entered - Please Specify Producer Hostname!')
          print("Command Line Error: No Producer Hostname Entered - Please Specify Producer Hostname!")
          sys.exit(1)

     try:
          #Get a handle to the Kafka Producer
          logger.debug("Producer's Name is: "+producerName)
          producer = KafkaProducer(bootstrap_servers=producerName, value_serializer=lambda v:json.dumps(v).encode('utf-8'))
     except KafkaError as err:
          logger.error(str(err))
          sys.exit(1)

     return producer

#Send Data to Kafka Broker
def SendDataToBroker(topic, stats, logger):
     omrfProducer = GetProducer(logger)

     try:
          omrfProducer.send(topic="omrf-advanced-gpu-stats", value=stats)
          omrfProducer.flush()

          #Close the Kafka Producer
          omrfProducer.close()

     except KafkaError as err:
          logger.error(str(err))

#Initilize NVML Environment
def NvmlInit(logger):
     try:
           nvmlInit()
     except NVMLError as err:
           #NMVL did not initialize - log error
           logger.error(nvmlErrorString(err))

################################################################################
#Get Memory ECC Errors                                                         #
#                                                                              #
#Function: GetMemoryECCStats() issues nvmlDeviceGetMemoryErrorCounter() API    #
#          cycling through the counter, error, and location types to produce   #
#          a dictionary of GPU Memory ECC error counts.                        #
#                                                                              #
#Returns:  Dictionary containing memory ECC error counts.                      #
#                                                                              #
#Calls:    nvmlDeviceGetMemoryErrorCounter(handle, errorType, counterType,     #
#          locationType).                                                      #
#                                                                              #
#Variables:                                                                    #
#  handle         = GPU Device Handle acquired by index                        #
#  errortype      = Volatile or Aggregate ECC strings                          #
#  counter        = Corrected(Single Bit) and UnCorrected(Double Bit) Enum     #
#  logger         = OMRF specific logger                                       #
#                   Logs are found in /home/<yours>/omrf/logs/omrf.log         #
################################################################################
def GetMemoryECCStats(handle, errorType, counter, location, logger):
     #Get the Location and Counter Names and Values
     locationType = location.value
     locationName = location.name
     counterType  = counter.value
     counterName  = counter.name

     #Create the gpuStats dictionary to pass back to the caller
     gpuStats = {}

     #Acquire Memory ECC Errors
     try:
          memECC = nvmlDeviceGetMemoryErrorCounter(handle, errorType, counterType, locationType)
     except NVMLError as err:
          if (err.value == NVML_ERROR_NOT_SUPPORTED):
               #Mark the memECC result as "N/A"
               memECC = "N/A"
          else:
               #log the error
               logger.error("NVML Error: " + err.value)

     #Add the data to gpuStats
     gpuStats[gpu + "-" + counterName + "-" + locationName] = memECC


     return gpuStats


################################################################################
#Collect GPU Stats                                                             #
#                                                                              #
#Function: CollectGpuStats uses various helper functions to pull all relevant  #
#          metric data and places all data into gpuStats dictionary to be      #
#          passed to the Kafka Broker using the "advance-gpu-stats" topic.     #
#                                                                              #
#Calls:    GetMemoryECCStats(handle, sbErrorType, memCounterType, memLocation, #
#                            logger)                                           #
#                                                                              #
#Returns:  Dictionary containing memory ECC error counts.                      #
#                                                                              #
#Variables:                                                                    #
#  handle         = GPU Device Handle acquired by index                        #
#  logger         = OMRF specific logger                                       #
#                   Logs are found in /home/<yours>/omrf/logs/omrf.log         #
################################################################################
def CollectGPUStats(handle, logger):
     #Create the Stats Dictionary to return to the caller
     stats = {}

     #Acquire the number of Counter Types for this GPU
     counterTypeCount = Memory_Counter_Type.NVML_ECC_COUNTER_TYPE_COUNT.value

     #initialize j to zero
     j = 0
     for j in range (counterTypeCount):
          #Set memCounterType
          memCounterType = Memory_Counter_Type(j)

          #Acquire the number of Memory Locations for this GPU
          locCount = Memory_Location.NVML_MEMORY_LOCATION_COUNT.value

          #Initialize k to zero
          k = 0
          for k in range(locCount):

               #Set Memory Location
               memLocation = Memory_Location(k)

               #Collect the Stats for Volatile and Aggregate ECCs
               stats.update(GetMemoryECCStats(handle, sbErrorType, memCounterType, memLocation, logger))
               stats.update(GetMemoryECCStats(handle, dbErrorType, memCounterType, memLocation, logger))

     return stats


#############################
####         Main        ####
#############################

#Create initial Logger and Log entry
omrfLogger = OMRFLogger('OMRF.ADVANCED','/home/ruhlowr/omrf/logs/omrf.log')
omrfLogger.info('Advanced Data Collection Initiated')

#Acquire hostname for this stats collection
hostname = socket.gethostname()

#Aquire Current Formatted Time
currentTime = GetFormattedCurrentTime()

#Initialize the NVML Environment
NvmlInit(omrfLogger)

#Create Dictionaires to hold GPU Stats
omrfGpuStats = {}

#Enter Time, Hostname, and Driver Version to gpuStats dictionary for output
omrfGpuStats["@timestamp"] = currentTime
omrfGpuStats["hostname"] = hostname
omrfGpuStats["driver-version"] = str(nvmlSystemGetNVMLVersion().decode())

#Acquire the number of GPU devices
deviceCount = nvmlDeviceGetCount()

#For each device, collect the stats
for i in range(deviceCount):

       try:
            #Acquire the handle for device "i"
            omrfHandle = nvmlDeviceGetHandleByIndex(i)

            #Identify the GPU
            gpu = "gpu"+str(i)

            #Collect GPU Memory ECC stats for each device
            omrfGpuStats.update(CollectGPUStats(omrfHandle, omrfLogger))

       except NVMLError as err:
            omrfLogger.error(nvmlErrorString(err))

#Shutdown NVML
nvmlShutdown()

#Send the output to Kafka Broker using "omrf-advanced-gpu-stats" topic
topic = 'omrf-advanced-gpu-stats'
SendDataToBroker(topic, omrfGpuStats, omrfLogger)

#Create completion entry in Log
omrfLogger.info('Advanced Data Collection Completed')
