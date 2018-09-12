'''
Created on May 21, 2013

@author: temp_dmenes
'''
import os
from cStringIO import StringIO
from airassessmentreporting.airutility import drop_assembly_if_exists
from airassessmentreporting.airutility import RunContext

__all__ = ['prep_sql_server']

THIS_DIR = os.path.dirname( os.path.abspath( __file__ ) )

ALLOW_AD_HOC_QUERIES = '''
sp_configure 'show advanced options', 1
GO
RECONFIGURE
GO
sp_configure 'ad hoc distributed queries', 1
RECONFIGURE
GO
EXEC master.dbo.sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0', N'AllowInProcess', 1
GO
EXEC master.dbo.sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0', N'DynamicParameters', 1
GO
'''

CREATE_TO_PROPER_CASE_FUNCTION = '''
CREATE ASSEMBLY [ToProperCase]
    AUTHORIZATION [dbo]
    FROM 0x4D5A90000300000004000000FFFF0000B800000000000000400000000000000000000000000000000000000000000000000000000000000000000000800000000E1FBA0E00B409CD21B8014CCD21546869732070726F6772616D2063616E6E6F742062652072756E20696E20444F53206D6F64652E0D0D0A2400000000000000504500004C010300AE8412520000000000000000E00002210B010800000E000000060000000000004E2D0000002000000040000000004000002000000002000004000000000000000400000000000000008000000002000000000000030040850000100000100000000010000010000000000000100000000000000000000000F82C000053000000004000000004000000000000000000000000000000000000006000000C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000200000080000000000000000000000082000004800000000000000000000002E74657874000000540D000000200000000E000000020000000000000000000000000000200000602E7273726300000000040000004000000004000000100000000000000000000000000000400000402E72656C6F6300000C0000000060000000020000001400000000000000000000000000004000004200000000000000000000000000000000302D0000000000004800000002000500F0220000080A00000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000133004008A020000010000110F00280E00000A2C067E0F00000A2A0F00281000000A6F1100000A6F1200000A0A1F0E0B0F01281300000A13111211281400000A1201281500000A2C0907163205071F1E310B7201000070731600000A7A72C40000700C731700000A0D08178D1600000113121112161F2C9D11126F1800000A13041613052B1109110411059A6F1900000A11051758130511050732EA1E8D11000001131311131672BB010070A211131772BF010070A211131872C3010070A211131972C7010070A211131A72CB010070A211131B72CF010070A211131C72D3010070A211131D72D7010070A2111313061106731A00000A2672DB0100701307110613141613152B18111411159A130811071108281B00000A1307111517581315111511148E6932E0110772E1010070281B00000A1307061107281C00000A1309731700000A130A16130B2B5A1109110B9A7E1D00000A281E00000A2C431109110B9A72BB010070281F00000A2C26110A6F2000000A16311C110A110A6F2000000A17596F2100000A72BB010070281F00000A2D0C110A1109110B9A6F1900000A110B1758130B110B11098E69329E72F5010070130C16130D38C3000000110A110D6F2100000A130E110E166F2200000A13161216282300000A6F2400000A130F110E6F2500000A173111110F110E176F2600000A281B00000A130F09110E6F2700000A2C09110E6F2400000A130F110A110D110F6F2800000A110A110D6F2100000A130E72F50100701310110D110A6F2000000A17592F0D110A110D17586F2100000A1310110C110E281B00000A130C110E72BF010070281F00000A2C1C111072BB010070281E00000A2C0E110C72BB010070281B00000A130C110D1758130D110D110A6F2000000A3F2FFFFFFF110C732900000A2A1E02282A00000A2A000042534A4201000100000000000C00000076322E302E35303732370000000005006C000000B8020000237E0000240300008403000023537472696E677300000000A8060000F801000023555300A0080000100000002347554944000000B00800005801000023426C6F620000000000000002000001471502080900000000FA25330016000001000000180000000200000002000000020000002A0000000B0000000100000001000000010000000300000000000A000100000000000600400039000A00680053000A00720053000600AD009B000600C4009B000600E1009B00060000019B00060019019B00060032019B0006004D019B00060068019B000600A00181010600B4019B000600ED01CD0106000D02CD010A0046022B0206007E02390006008D0239000600930239000600A20239000600C702AC020600CE0239000600DD02AC020E001103F2020000000001000000000001000100010010001B00000005000100010050200000000096007B000A000100E622000000008618880013000300000001008E00000002009200210088001700290088001700310088001700390088001700410088001700490088001700510088001700590088001700610088001C0069008800170071008800210079008800130081008800130011005B022B00110066022F0011006B02330009007502330089008502330019006B023700910075023300990099023B00A100880017000C00880013008900D30248000C00D9024F000C00880055008900EB025F00C100D3026500890017036C0089001D036F0089002B036F000C00370337000C004103750089004A037B00B1007502330089005403330089005C0337008900670380000C00710385000C007A038B0011008800170009008800130020006B0026002E002B00BA002E001300CC002E001B00CC002E002300D2002E000B00BA002E003300F8002E003B00CC002E004B00CC002E005B0030012E006300390192004200048000000100000073132F6F0000000000007B0000000200000000000000000000000100300000000000020000000000000000000000010047000000000002000000000000000000000001003900000000000000003C4D6F64756C653E00546F50726F706572436173652E646C6C0055736572446566696E656446756E6374696F6E73006D73636F726C69620053797374656D004F626A6563740053797374656D2E446174610053797374656D2E446174612E53716C54797065730053716C537472696E670053716C496E74333200546F50726F70657243617365002E63746F7200737472006D6178526F6D616E0053797374656D2E5265666C656374696F6E00417373656D626C795469746C6541747472696275746500417373656D626C794465736372697074696F6E41747472696275746500417373656D626C79436F6E66696775726174696F6E41747472696275746500417373656D626C79436F6D70616E7941747472696275746500417373656D626C7950726F6475637441747472696275746500417373656D626C79436F7079726967687441747472696275746500417373656D626C7954726164656D61726B41747472696275746500417373656D626C7943756C747572654174747269627574650053797374656D2E52756E74696D652E496E7465726F70536572766963657300436F6D56697369626C6541747472696275746500417373656D626C7956657273696F6E4174747269627574650053797374656D2E52756E74696D652E436F6D70696C6572536572766963657300436F6D70696C6174696F6E52656C61786174696F6E734174747269627574650052756E74696D65436F6D7061746962696C697479417474726962757465004D6963726F736F66742E53716C5365727665722E5365727665720053716C46756E6374696F6E417474726962757465006765745F49734E756C6C004E756C6C006765745F56616C756500546F537472696E6700537472696E6700546F4C6F77657200496E74333200496E74313600547279506172736500457863657074696F6E0053797374656D2E436F6C6C656374696F6E732E47656E65726963004C697374603100436861720053706C6974004164640049456E756D657261626C65603100436F6E6361740053797374656D2E546578742E526567756C617245787072657373696F6E7300526567657800456D707479006F705F496E657175616C697479006F705F457175616C697479006765745F436F756E74006765745F4974656D006765745F436861727300546F5570706572006765745F4C656E67746800537562737472696E6700436F6E7461696E73007365745F4974656D00000080C159006F00750020006D007500730074002000700072006F007600690064006500200061006E00200069006E007400650067006500720020006200650074007700650065006E0020003000200061006E006400200033003000200066006F007200200074006800650020007300650063006F006E006400200070006100720061006D006500740065007200200028006E0075006D0062006500720020006F006600200072006F006D0061006E0020006E0075006D006500720061006C00730029000080F569002C00690069002C006900690069002C00690076002C0076002C00760069002C007600690069002C0076006900690069002C00690078002C0078002C00780069002C007800690069002C0078006900690069002C007800690076002C00780076002C007800760069002C0078007600690069002C00780076006900690069002C007800690078002C00780078002C007800780069002C0078007800690069002C00780078006900690069002C0078007800690076002C007800780076002C0078007800760069002C00780078007600690069002C007800780076006900690069002C0078007800690078002C007800780078000003200000032C0000032E0000032700010328000003290000032D0001032F00000528005B0000135D0029007C0028005C0062006D0063002900000100000A705D8F1545F94583211B3633EE7B020008B77A5C561934E08908000211091109110D03200001042001010E04200101020420010108040100000003200002030611090320000E03200008060002020E100605151255010E0620011D0E1D030520010113000920010115125D0113000500020E0E0E0600021D0E0E0E02060E050002020E0E05200113000804200103080420010E08052001021300062002010813002707170E060E151255010E1D0E081D0E0E0E1D0E151255010E080E080E0E0E081D031D0E1D0E08031101000C546F50726F70657243617365000005010000000025010020416D65726963616E20496E737469747574657320666F72205265736561726368000037010032436F7079726967687420C2A920416D65726963616E20496E737469747574657320666F72205265736561726368203230313300000801000800000000001E01000100540216577261704E6F6E457863657074696F6E5468726F777301202D000000000000000000003E2D0000002000000000000000000000000000000000000000000000302D000000000000000000000000000000005F436F72446C6C4D61696E006D73636F7265652E646C6C0000000000FF25002040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100100000001800008000000000000000000000000000000100010000003000008000000000000000000000000000000100000000004800000058400000A40300000000000000000000A40334000000560053005F00560045005200530049004F004E005F0049004E0046004F0000000000BD04EFFE00000100000001002F6F7313000001002F6F73133F000000000000000400000002000000000000000000000000000000440000000100560061007200460069006C00650049006E0066006F00000000002400040000005400720061006E0073006C006100740069006F006E00000000000000B00404030000010053007400720069006E006700460069006C00650049006E0066006F000000E0020000010030003000300030003000340062003000000064002100010043006F006D00700061006E0079004E0061006D0065000000000041006D00650072006900630061006E00200049006E0073007400690074007500740065007300200066006F0072002000520065007300650061007200630068000000000044000D000100460069006C0065004400650073006300720069007000740069006F006E000000000054006F00500072006F0070006500720043006100730065000000000040000F000100460069006C006500560065007200730069006F006E000000000031002E0030002E0034003900370039002E00320038003400360033000000000044001100010049006E007400650072006E0061006C004E0061006D006500000054006F00500072006F0070006500720043006100730065002E0064006C006C00000000008800320001004C006500670061006C0043006F007000790072006900670068007400000043006F0070007900720069006700680074002000A900200041006D00650072006900630061006E00200049006E0073007400690074007500740065007300200066006F0072002000520065007300650061007200630068002000320030003100330000004C00110001004F0072006900670069006E0061006C00460069006C0065006E0061006D006500000054006F00500072006F0070006500720043006100730065002E0064006C006C00000000003C000D000100500072006F0064007500630074004E0061006D0065000000000054006F00500072006F0070006500720043006100730065000000000044000F000100500072006F006400750063007400560065007200730069006F006E00000031002E0030002E0034003900370039002E00320038003400360033000000000048000F00010041007300730065006D0062006C0079002000560065007200730069006F006E00000031002E0030002E0034003900370039002E00320038003400360033000000000000000000002000000C000000503D00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
    WITH PERMISSION_SET = SAFE 
GO
ALTER ASSEMBLY [ToProperCase]
    ADD FILE FROM 0xEFBBBF7573696E672053797374656D2E5265666C656374696F6E3B0D0A7573696E672053797374656D2E52756E74696D652E436F6D70696C657253657276696365733B0D0A7573696E672053797374656D2E52756E74696D652E496E7465726F7053657276696365733B0D0A7573696E672053797374656D2E446174612E53716C3B0D0A0D0A2F2F2047656E6572616C20496E666F726D6174696F6E2061626F757420616E20617373656D626C7920697320636F6E74726F6C6C6564207468726F7567682074686520666F6C6C6F77696E670D0A2F2F20736574206F6620617474726962757465732E204368616E6765207468657365206174747269627574652076616C75657320746F206D6F646966792074686520696E666F726D6174696F6E0D0A2F2F206173736F636961746564207769746820616E20617373656D626C792E0D0A5B617373656D626C793A20417373656D626C795469746C652822546F50726F7065724361736522295D0D0A5B617373656D626C793A20417373656D626C794465736372697074696F6E282222295D0D0A5B617373656D626C793A20417373656D626C79436F6E66696775726174696F6E282222295D0D0A5B617373656D626C793A20417373656D626C79436F6D70616E792822416D65726963616E20496E737469747574657320666F7220526573656172636822295D0D0A5B617373656D626C793A20417373656D626C7950726F647563742822546F50726F7065724361736522295D0D0A5B617373656D626C793A20417373656D626C79436F707972696768742822436F7079726967687420C2A920416D65726963616E20496E737469747574657320666F72205265736561726368203230313322295D0D0A5B617373656D626C793A20417373656D626C7954726164656D61726B282222295D0D0A5B617373656D626C793A20417373656D626C7943756C74757265282222295D0D0A0D0A5B617373656D626C793A20436F6D56697369626C652866616C7365295D0D0A0D0A2F2F0D0A2F2F2056657273696F6E20696E666F726D6174696F6E20666F7220616E20617373656D626C7920636F6E7369737473206F662074686520666F6C6C6F77696E6720666F75722076616C7565733A0D0A2F2F0D0A2F2F2020202020204D616A6F722056657273696F6E0D0A2F2F2020202020204D696E6F722056657273696F6E0D0A2F2F2020202020204275696C64204E756D6265720D0A2F2F2020202020205265766973696F6E0D0A2F2F0D0A2F2F20596F752063616E207370656369667920616C6C207468652076616C756573206F7220796F752063616E2064656661756C7420746865205265766973696F6E20616E64204275696C64204E756D626572730D0A2F2F206279207573696E672074686520272A272061732073686F776E2062656C6F773A0D0A5B617373656D626C793A20417373656D626C7956657273696F6E2822312E302E2A22295D0D0A0D0A AS N'Properties\AssemblyInfo.cs', 0xEFBBBF7573696E672053797374656D3B0D0A7573696E672053797374656D2E446174613B0D0A7573696E672053797374656D2E446174612E53716C436C69656E743B0D0A7573696E672053797374656D2E446174612E53716C54797065733B0D0A7573696E672053797374656D2E546578743B0D0A7573696E672053797374656D2E546578742E526567756C617245787072657373696F6E733B0D0A7573696E67204D6963726F736F66742E53716C5365727665722E5365727665723B0D0A7573696E672053797374656D2E436F6C6C656374696F6E732E47656E657269633B0D0A2F2F7573696E672053797374656D2E4C696E713B0D0A2F2A0D0A202A20596F75206E65656420746F206275696C642074686973206265666F72652072756E6E696E6720746865206E657874207363726970742E200D0A202A20596F752077696C6C206861766520746F2072756E207468697320746F20696D706C656D656E743A0D0A202A20555345205B6D796E657764625D0D0A202A20474F0D0A202A2073705F636F6E6669677572652027636C7220656E61626C65272C20310D0A202A20474F0D0A202A205245434F4E464947555245202D2D57495448204F564552524944450D0A202A20474F0D0A202A202D2D6174207468697320706F696E7420796F75206861766520746F20726573746172742053514C2053657276657220736572766963652C207468656E20676574206261636B20696E20616E642072756E207468652062656C6F7720636F6465207265706C6163696E672074686520706C616365206F662074686520646C6C0D0A202A2043524541544520415353454D424C59205B546F50726F70436173655D0D0A202A20415554484F52495A4154494F4E205B64626F5D0D0A202A2046524F4D2027433A5C55736572735C5A536368726F656465725C446F63756D656E74735C56697375616C2053747564696F20323031305C50726F6A656374735C546F50726F70436173655C546F50726F70436173655C62696E5C44656275675C546F50726F70436173652E646C6C270D0A202A2057495448205045524D495353494F4E5F534554203D20534146450D0A202A20474F0D0A202A200D0A202A20414C534F204E4F54453A2054686973206973206265696E67206275696C7420696E202E4E6574204672616D65776F726B20322E302062656361757365207468617427732077686174206D79206C6F63616C2053514C205365727665722072756E732E0D0A202A2F0D0A7075626C6963207061727469616C20636C6173732055736572446566696E656446756E6374696F6E730D0A7B0D0A202020205B4D6963726F736F66742E53716C5365727665722E5365727665722E53716C46756E6374696F6E5D0D0A202020207075626C6963207374617469632053716C537472696E6720546F50726F706572436173652853716C537472696E67207374722C2053716C496E743332206D6178526F6D616E290D0A202020207B0D0A2020202020202020696620287374722E49734E756C6C290D0A20202020202020202020202072657475726E2053716C537472696E672E4E756C6C3B0D0A2020202020202020537472696E6720696E707574537472696E67203D207374722E56616C75652E546F537472696E6728292E546F4C6F77657228293B202F2F206C6F776572636173652065766572797468696E670D0A202020202020202073686F7274206E756D526F6D616E73203D2031343B0D0A20202020202020206966202821496E7431362E5472795061727365286D6178526F6D616E2E56616C75652E546F537472696E6728292C206F7574206E756D526F6D616E7329207C7C206E756D526F6D616E73203C2030207C7C206E756D526F6D616E73203E203330290D0A2020202020202020202020207468726F77206E657720457863657074696F6E2822596F75206D7573742070726F7669646520616E20696E7465676572206265747765656E203020616E6420333020666F7220746865207365636F6E6420706172616D6574657220286E756D626572206F6620726F6D616E206E756D6572616C732922293B0D0A0D0A2020202020202020537472696E6720726F6D616E73203D2022692C69692C6969692C69762C762C76692C7669692C766969692C69782C782C78692C7869692C786969692C7869762C78762C7876692C787669692C78766969692C7869782C78782C7878692C787869692C78786969692C787869762C7878762C787876692C78787669692C7878766969692C787869782C787878223B0D0A20202020202020202F2F4C6973743C537472696E673E20726F6D616E4C697374203D20726F6D616E732E53706C697428272C27292E546F4C69737428293B0D0A20202020202020202F2F696D706C656D656E746174696F6E20666F72202E6E657420322E302073696E636520697420646F65736E277420686176652053797374656D2E4C696E710D0A20202020202020204C6973743C537472696E673E20726F6D616E4C697374203D206E6577204C6973743C537472696E673E28293B0D0A2020202020202020537472696E675B5D20746D70417272203D20726F6D616E732E53706C697428272C27293B0D0A2020202020202020666F722028696E742069203D20303B2069203C206E756D526F6D616E733B20692B2B290D0A202020202020202020202020726F6D616E4C6973742E41646428746D704172725B695D293B0D0A0D0A20202020202020202F2F72656D6F76652074686520726F6D616E206E756D6572616C7320616674657220746865206E756D626572207370656369666965642062792074686520757365720D0A20202020202020202F2F726F6D616E4C6973742E52656D6F766552616E6765286E756D526F6D616E732C20726F6D616E4C6973742E436F756E74202D206E756D526F6D616E73293B0D0A0D0A20202020202020202F2F6172726179206F66206368617261637465727320746F2073706C6974206F6E0D0A2020202020202020537472696E675B5D207370656369616C43686172616374657273203D206E657720537472696E675B5D207B202220222C20222C222C20222E222C202227222C202228222C202229222C20222D222C20222F22207D3B0D0A20202020202020204C6973743C537472696E673E207370656369616C436861724C697374203D206E6577204C6973743C737472696E673E287370656369616C43686172616374657273293B0D0A0D0A20202020202020202F2F2073706C697474696E67206F6E2065616368206368617261637465722066726F6D207370656369616C436861726163746572732C20616E64206B656570696E67207468652063686172616374657273206166746572207468652073706C69740D0A2020202020202020737472696E67207061747465726E203D204022285B223B0D0A2020202020202020666F72656163682028537472696E67207320696E207370656369616C43686172616374657273290D0A2020202020202020202020207061747465726E202B3D20733B0D0A20202020202020202F2F20416C736F206C6F6F6B696E6720666F72204D634465726D6F6E202D2D2054686174277320776861742074686520227C285C626D63292220646F65732E204F6E6C79206E65656420746F20636865636B20666F720D0A20202020202020202F2F206C6F776572636173652073696E6365207765206C6F736572636173652074686520656E7469726520737472696E6720617420626567696E6E696E67206F662072756E0D0A20202020202020207061747465726E202B3D2040225D297C285C626D6329223B0D0A0D0A20202020202020202F2F4C6973743C537472696E673E20776F72644C697374203D2052656765782E53706C697428696E707574537472696E672C207061747465726E292E57686572652873203D3E20732E5472696D282920213D20537472696E672E456D707479292E546F4C69737428293B0D0A2020202020202020537472696E675B5D20746D70576F726473203D2052656765782E53706C697428696E707574537472696E672C207061747465726E293B0D0A20202020202020204C6973743C537472696E673E20776F72644C697374203D206E6577204C6973743C737472696E673E28293B0D0A20202020202020202F2F636F7079696E67206F6E6C792022776F72647322202D2D2074686174206973206E6F6E2D656D70747920656E74726965730D0A2020202020202020666F722028696E742069203D20303B2069203C20746D70576F7264732E4C656E6774683B20692B2B290D0A20202020202020207B0D0A20202020202020202020202069662028746D70576F7264735B695D20213D20537472696E672E456D707479290D0A2020202020202020202020207B0D0A202020202020202020202020202020202F2F72656D6F7665206578747261207370616365732E20496620746865726520697320616C72656164792061207370616365207468656E2069676E6F726520616C6C207768697465200D0A202020202020202020202020202020202F2F737061636520616674657220756E74696C2077652067657420746F2061206368617261637465720D0A20202020202020202020202020202020696620282128746D70576F7264735B695D203D3D2022202220262620776F72644C6973742E436F756E74203E203020262620776F72644C6973745B776F72644C6973742E436F756E74202D20315D203D3D2022202229290D0A2020202020202020202020202020202020202020776F72644C6973742E41646428746D70576F7264735B695D293B0D0A2020202020202020202020207D0D0A20202020202020207D0D0A0D0A2020202020202020537472696E67206F7574537472696E67203D2022223B0D0A0D0A20202020202020202F2F676F207468726F756768206561636820776F7264207765206765742028776520616C736F2073706C6974206F6E20706572696F647320616E642061706F7374726F70686573290D0A20202020202020202F2F20616E642075707065726361736520746865206669727374206C65747465720D0A2020202020202020666F722028696E742069203D20303B2069203C20776F72644C6973742E436F756E743B20692B2B290D0A20202020202020207B0D0A202020202020202020202020537472696E6720656C656D203D20776F72644C6973745B695D3B0D0A2020202020202020202020202F2F7570706572636173696E67206669727374206C657474657220696E207468652022776F7264220D0A202020202020202020202020537472696E67206E6577456C656D203D20656C656D5B305D2E546F537472696E6728292E546F557070657228293B0D0A20202020202020202020202069662028656C656D2E4C656E677468203E2031290D0A202020202020202020202020202020206E6577456C656D202B3D20656C656D2E537562737472696E672831293B0D0A2020202020202020202020202F2F636865636B696E6720666F7220726F6D616E206E756D6572616C730D0A20202020202020202020202069662028726F6D616E4C6973742E436F6E7461696E7328656C656D29290D0A202020202020202020202020202020206E6577456C656D203D20656C656D2E546F557070657228293B0D0A2020202020202020202020202F2F757064617465207468652061727261792077697468207468652070726F70657220636173656420656C656D656E740D0A202020202020202020202020776F72644C6973745B695D203D206E6577456C656D3B0D0A0D0A2020202020202020202020202F2F6E6F7720746865206C6F67696320666F7220616464696E67207468697320656C656D656E7420746F20746865206F757470757420737472696E670D0A202020202020202020202020656C656D203D20776F72644C6973745B695D3B0D0A202020202020202020202020537472696E67206E657874203D2022223B202F2F206E65787420656C656D656E7420696E207468652061727261792E205468697320697320746865206E6578742022776F7264220D0A0D0A2020202020202020202020202F2F646F6E27742077616E7420746F20676F206F7665722074686520626F756E6473206F66207468652061727261790D0A2020202020202020202020206966202869203C20776F72644C6973742E436F756E74202D2031290D0A202020202020202020202020202020206E657874203D20776F72644C6973745B69202B20315D3B0D0A0D0A2020202020202020202020206F7574537472696E67202B3D20656C656D3B0D0A0D0A2020202020202020202020202F2F616464696E6720737061636520616674657220636F6D6D61206966206E65636573736172790D0A20202020202020202020202069662028656C656D203D3D20222C22202626206E65787420213D20222022290D0A202020202020202020202020202020206F7574537472696E67202B3D202220223B0D0A0D0A2020202020202020202020202F2F6966207468652063757272656E742022776F726422206973206E6F7420616E2061706F7374726F7068652C2068797068656E2C206F72206120706572696F6420616E6420746865206E6578742022776F726422206973206E6F74200D0A2020202020202020202020202F2F20616E2061706F7374726F7068652C206120706572696F642C20612068797068656E2C206F72206120636F6D6D61207468656E207765206D7573742061646420612073706163650D0A2020202020202020202020202F2F69662028656C656D20213D2022272220262620656C656D20213D20222E2220262620656C656D20213D20222D22202626206E65787420213D20222722202626206E65787420213D20222E22202626206E65787420213D20222C22202626206E65787420213D20222D22290D0A2020202020202020202020202F2F202020202020202020202020202020206966202828217370656369616C436861724C6973742E436F6E7461696E7328656C656D2920262620217370656369616C436861724C6973742E436F6E7461696E73286E6578742929207C7C20656C656D203D3D20222C22290D0A2020202020202020202020202F2F20202020202020202020202020202020202020206F7574537472696E67202B3D202220223B0D0A20202020202020207D0D0A202020202020202072657475726E206E65772053716C537472696E67286F7574537472696E67293B0D0A202020207D0D0A7D3B0D0A0D0A  
    AS N'ToPropCase.cs'
GO 
CREATE FUNCTION [dbo].[ToProperCase](@str [nvarchar](4000), @maxRoman [int])
    RETURNS [nvarchar](4000) WITH EXECUTE AS CALLER
    AS 
    EXTERNAL NAME [ToProperCase].[UserDefinedFunctions].[ToProperCase]
GO
EXEC sys.sp_addextendedproperty @name=N'SqlAssemblyFile', @value=N'ToProperCase.cs' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'FUNCTION',@level1name=N'ToProperCase'
GO
EXEC sys.sp_addextendedproperty @name=N'SqlAssemblyFileLine', @value=N'32' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'FUNCTION',@level1name=N'ToProperCase'
GO
'''

CREATE_NAME_SIMILARITY_FILE = os.path.join( THIS_DIR, 'NameSimilarity.sql' )


def prep_sql_server( db_context ):
    '''
    Run this method on a server to configure it and pre-load UDFs
    '''
    
    #configure DB to use C# functions
    #db_context.executeNoResults( "sp_configure 'clr enable', 1" )
    #db_context.executeNoResults( "RECONFIGURE --WITH OVERRIDE" )
    
    # adding ToProperCase UDF - if the code changes you must rebuild and deploy and
    #                             replace the FROM statements here
    drop_assembly_if_exists( "ToProperCase", db_context )
    f = StringIO( CREATE_TO_PROPER_CASE_FUNCTION )
    db_context.executeFile( f, True )

    drop_assembly_if_exists( "NameSimilarity", db_context )
    f = open( CREATE_NAME_SIMILARITY_FILE, 'r' )
    db_context.executeFile( f, True )
    
if __name__ == '__main__':
    runContextName = 'OGT_12SP'
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    prep_sql_server(dbContext)
    

