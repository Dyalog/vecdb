 MakeBoot;Path
⍝ Built the "vecdbboot" workspace

 Path←{(1-⌊/'/\'⍳⍨⌽⍵)↓⍵}4↓,¯1↑⎕CR⊃⎕SI
 ⎕SE.SALT.Load Path,'BootServers.dyalog'
 ⎕LX←'BootServers '''''
 ⎕←'Now please:'
 ⎕←'      ⎕EX ''MakeBoot'''
 ⎕←'      )WSID ',Path,'vecdbboot.dws'
 ⎕←'      )SAVE'
