 BootServers dummy;port;getenv;getnum;path
⍝ Start a vecdb server process if VECDBSRV="config.json" PORT=nnnn
⍝         vecdb slave process if  VECDBSLAVE="file" SHARDS="n" PORT=nnnn

 getenv←{0=≢2 ⎕NQ'.' 'GetEnvironment'⍵:⍺}
 getnum←{⊃2⊃⎕VFI ⍵}
 path←'file://',⊃⎕NPARTS ⎕WSID

 VECDBSRV←0≠⍴CONFIG←''getenv'VECDBSRV'
 VECDBSLAVE←0≠VECDB←''getenv'VECDBSLAVE'
 port←getnum''getenv'PORT'

 2 ⎕FIX path,'APLProcess.dyalog'
 2 ⎕FIX path,'vecdb.dyalog'
 2 ⎕FIX path,'vecdbsrv.dyalog'

 :If 0=⎕NC'DRC' ⍝ Get conga if necessary
     'DRC'⎕CY'conga'getenv'CONGAWS'
 :EndIf

 :If 0=port
     ⎕←'See:'
     '      ',2 ⎕FIX path,'TestVecdb.dyalog'
     '      ',2 ⎕FIX path,'TestVecdbSrv.dyalog'
 :Else

     {}1 ##.DRC.Init''

     :If VECDBSRV ⋄ vecdbsrv.Start CONFIG port
     :ElseIf VECDBSLAVE ⋄ vecdbslave.Start VECDB port
     :Else
         ⎕←'Invalid configuration...'
     :EndIf
 :EndIf
