def qty_so_adjusted(data):
    if data['is_meccaya'] == True:
        return data['kuantitas'] * data['content']
    
    elif data['is_meccaya'] == False and (data['satuan']=='Lsn' or data['satuan']=='Ctn'):
        return data['kuantitas'] * data['content']
    
    else:
        return data['kuantitas'] / data['content']
    

def qty_inv_adjusted(data):
    if data['is_meccaya'] == True:
        return data['qty_faktur'] * data['content']
    
    elif data['is_meccaya'] == False and (data['satuan']=='Lsn' or data['satuan']=='Ctn'):
        return data['qty_faktur'] * data['content']
    
    else:
        return data['qty_faktur'] / data['content']


def qty_sj_adjusted(data):

    if data['id_barang'] == 'MCY.KS.K-001' and data['branch'] == 'DIY':
        return data['qty_faktur'] / 12
    
    elif (data['branch'] == 'DIY' and data['is_meccaya'] == True) and data['id_barang'] != 'MCY.KS.K-001':
        return data['qty_faktur'] * 1
    
    elif data['is_meccaya'] == True:
        return data['qty_faktur'] * data['content']
    
    elif data['is_meccaya'] == False and (data['satuan'] =='Lsn' or data['satuan'] == 'Ctn'):
        return data['qty_faktur'] * data['content']
    
    else:
        return data['qty_faktur'] / data['content']