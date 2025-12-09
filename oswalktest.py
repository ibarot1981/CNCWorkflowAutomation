import os

root = r"C:\Irshad\Safari\DRWGD\Nesting\DXF"
target = "JKM500WR_FldTypeBkt_BottomSuppPlate_8mm_2Nos.dxf".lower()

for r, d, files in os.walk(root):
    for f in files:
        if f.lower() == target:
            print("FOUND:", os.path.join(r,f))
