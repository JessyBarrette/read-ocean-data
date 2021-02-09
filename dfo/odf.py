import re
import pandas as pd


def read_odf(filename,
             header_end=' -- DATA -- ',
             ):
    """
    read_odf is a simple tool that  parse the header metadata and data from an DFO ODF file to a list of dictionaries.
    Data is also converted into a pandas data frame.
    :param filename: ODF file to read
    :param header_end: Expression used at the end of a ODF file header to define the end of the header and
     start of the data.
    :param parameter_section: ODF Header Section name that correspond to the variables specific attributes/metadata.
    :param output_column_name: Variable attribute in the parameter_section used to define the variables name.
    :return:
    """

    metadata = {}
    with open(filename, 'r') as f:
        line = ''

        # Read header one line at the time
        while not line.startswith(header_end):
            line = f.readline()
            # Drop some characters that aren't useful
            line = re.sub(r'\n|,$', '', line)

            # Sections
            if re.match(r'^\w', line):
                section = line.replace('\n', '').replace(',', '')
                if section not in metadata:
                    metadata.update({section: [{}]})
                else:
                    metadata[section].append({})

            # Dictionary type lines (key=value)
            elif re.match(r'^\s+.+=', line):  # Something=This
                dict_line = re.split(r'=', line, maxsplit=1)  # Make sure that only the first = is use to split
                dict_line = [re.sub(r'^\s+|\s+$', '', item) for item in dict_line]  # Remove trailing white spaces

                if re.match(r'\'.*\'', dict_line[1]):  # Is delimited by double quotes, definitely a string
                    dict_line[1] = str(re.sub(r'\'', '', dict_line[1]))
                    # TODO we should add special cases for dates and time to convert to datetime

                # Add to the metadata as a dictionary
                metadata[section][-1].update({dict_line[0]: dict_line[1]})

            elif re.match(r'^\s+.+', line):  # Unknown line format (likely comments)
                # TODO this hasn't been tested yet I haven't try an example with not a dictionary like line
                metadata[section].append(line)
            else:
                assert RuntimeError, "Can't understand the line: " + line

        # Assume all the lines below are data and read them all at once
        data_raw = f.readlines()

    # Simplify the single sections to a dictionary
    for section in metadata:
        if len(metadata[section]) == 1 and \
                type(metadata[section][0]) is dict:
            metadata[section] = metadata[section][0]
    return metadata, data_raw


def odf_dict_to_df(metadata,
                   data_raw,
                   parameter_section='PARAMETER_HEADER',
                   output_column_name='CODE',
                   variable_type='TYPE',
                   odf_type_to_pandas={'DOUB': float}
                   ):

    """
    :param metadata:
    :param data_raw:
    :param parameter_section: ODF Header Section name that correspond to the variables specific attributes/metadata.
    :param output_column_name: Variable attribute in the parameter_section used to define the variables name.
    :return:
    """

    # Convert it to a dataframe and use one of the fields in the parameter section to define the column's name
    if type(output_column_name) is str and output_column_name in metadata[parameter_section][0]:
        column_names = [parameter[output_column_name] for parameter in metadata[parameter_section]]
    elif type(output_column_name) is list and len(output_column_name)==len(metadata[parameter_section]):
        column_names = output_column_name
    else:
        assert RuntimeError, 'Unknown column name input format'+str(output_column_name)

    # Convert to dataframe
    df = pd.DataFrame(columns=column_names, data=[row.split() for row in data_raw])

    # Format columns dtypes based on the format specified in the header
    for parm in metadata[parameter_section]:
        # TODO there's likely a special case for datetime variables for which we should use pd.to_datetime instead
        if parm[variable_type] in odf_type_to_pandas:
            df[parm[output_column_name]] = df[parm[output_column_name]].astype(odf_type_to_pandas[parm[variable_type]])
        else:
            assert RuntimeError, 'Unknown Data Format: ' + parm[output_column_name] + '(' + parm[variable_type] + ')'
    return df


# TEST
testfile = r'C:\Users\jessy\Documents\repositories\cioos-siooc_data_transform\cioos_data_transform\mli_data\sample_data\2020-12-23\ODF_files_MLI\CTD_2019004_1_2A_DN.ODF'
metadata, raw_data = read_odf(testfile)

# Dataframe columns data type can be converted based on the PARAMETER_HEADER TYPE,
# would need to map ODF Types to Python types
df = odf_dict_to_df(metadata, raw_data)

# Convert to xarray
ds = df.to_xarray()

# Add variable attributes
# TODO Map metadata for each variables to the CIOOS, CF and ERDDAP standard ones
for var in metadata['PARAMETER_HEADER']:
    ds[var['CODE']].attrs = {'original_'+key: val for key, val in var.items()}

# Add global attributes
# TODO Extract some of the header metadata to be part of the CF recommended header metadata
for key in metadata:
    ds.attrs[key] = str(metadata[key])

# Reorder the data the same way than the initial data (may have to be change if we add more metadata variables)
ds = ds[df.columns]
# Save the xarray to a NetCDF format
ds.to_netcdf(testfile[:-4]+'.nc', mode='w')