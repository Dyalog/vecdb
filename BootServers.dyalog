 BootServers dummy;port;getenv;getnum;path
⍝ Start a vecdb server process if VECDBSRV="config.json" PORT=nnnn
⍝         vecdb slave process if  VECDBSLAVE="file" SHARDS="n" PORT=nnnn

 ⎕←'Command Line:'
 ⎕←2 ⎕NQ'.' 'GetCommandLine'
 getenv←{0=≢r←2 ⎕NQ'.' 'GetEnvironment'⍵:⍺ ⋄ r}
 getnum←{⊃2⊃⎕VFI ⍵}
 path←'file://',⊃⎕NPARTS ⎕WSID

 VECDBSRV←0≠≢SRVDB←''getenv'VECDBSRV'
 VECDBSLAVE←0≠≢VECDB←''getenv'VECDBSLAVE'
 SHARDS←2⊃⎕VFI''getenv'SHARDS'
 TOKEN←2⊃⎕VFI''getenv'TOKEN'

 port←getnum''getenv'PORT'

 2 ⎕FIX path,'APLProcess.dyalog'
 2 ⎕FIX path,'vecdb.dyalog'
 2 ⎕FIX path,'vecdbclt.dyalog'
 2 ⎕FIX path,'vecdbsrv.dyalog'
 2 ⎕FIX path,'vecdbslave.dyalog'

 :If 0=⎕NC'DRC' ⍝ Get conga if necessary
     'DRC'⎕CY'conga'getenv'CONGAWS'
 :EndIf

 :If 0=port
     ⎕←'See:'
     '      ',2 ⎕FIX path,'TestVecdb.dyalog'
     '      ',2 ⎕FIX path,'TestVecdbSrv.dyalog'
 :Else

     AUTOSHUT←1
     {}1 ##.DRC.Init''

     :If VECDBSRV ⋄ vecdbsrv.Start SRVDB port
     :ElseIf VECDBSLAVE ⋄ vecdbslave.Start VECDB SHARDS port
     :Else
         ⎕←'Invalid configuration...'
     :EndIf
 :EndIf
