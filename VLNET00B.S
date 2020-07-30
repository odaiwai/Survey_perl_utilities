; Do not change filenames or add or remove FILEI/FILEO statements using an editor. Use Cube/Application Manager.
RUN PGM=NETWORK PRNFILE="C:\SYDNEY_WCX_v0\temp\Outputs_am.PRN"
FILEO NETO = "C:\SYDNEY_WCX_v0\temp\Outputs_am.NET"
FILEI LINKI[2] = "C:\SYDNEY_WCX_v0\data_files\Observed\OBSAM.csv",
    VAR=Site(c),Dir(c),a, b, RName(C), Movm(c),CarLCV,RIG,Art,Tot
FILEO LINKO = "C:\SYDNEY_WCX_v0\temp\Outputs_am.dbf"
FILEI LINKI[1] = "{SCENARIO_DIR}\{year}{TimePeriod}_loaded_network.net"

PROCESS  PHASE=LINKMERGE  ; convert model to vehicles and compare where observed data available  
TotModVeh = V1_1 + V2_1 + V3_1 + V4_1 + V5_1/2 + V6_1/4 + V11_1 + V12_1 + V13_1 + V14_1 + V15_1/2 + V16_1/4 
IF  (li.2.Tot > 0)   ; note observed data is for 2 hors
  DIFTot = TotModVeh - li.2.Tot/2 
  GEHTot = SQRT(2*DIFTot^2/(TotModVeh+li.2.Tot/2))
  PTot = ABS(DIFTot) * 100 / (li.2.Tot/2)
  if (DIFTot < 0)
    MDIFTot = -1 * DIFTot
    DIFTot = 0
  ENDIF
ENDIF

ENDPROCESS

ENDRUN
