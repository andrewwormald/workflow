import * as React from 'react';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';

import './styles.css'
import StatusTimeline from "./Timeline";

function createData(
    WorkflowName: string,
    ForeignID: string,
    RunID: string,
    Status: string,
    IsStart: boolean,
    IsEnd: boolean,
    Object: string,
    CreatedAt: string,
) {
    return { WorkflowName, ForeignID, RunID, Status, IsStart, IsEnd, Object, CreatedAt };
}

const rows = [
    createData('Deposit', '159345798345', 'LSDKLJFN-SKDFJB-WERLTBE', 'Completed', false, true, '', '2023-07-13T01:03:55'),
    createData('SyncOnfido', '9823479283434', 'SLDKFNSS-SDFSDF-VBVXZSD', 'Awaiting report', false, false, '', '2023-07-13T01:03:55'),
    createData('UK customer onboarding with cooldown', '23874628937634', 'WERWEREH-OIUOIG-OPWKDCN', 'Waiting cooldown timeout',false, false,'', '2023-07-13T01:03:55'),
];


export default function BasicTable() {
    const myEntries = [
        {
            Timestamp: "13:03:05",
            Status: "Completed"
        }
    ]

    return (
        <TableContainer component={Paper}>
            <Table sx={{ minWidth: 650 }} aria-label="simple table">
                <TableHead>
                    <TableRow>
                        <TableCell>WorkflowName</TableCell>
                        <TableCell align="center">ForeignID</TableCell>
                        <TableCell align="center">RunID</TableCell>
                        <TableCell align="center">Status</TableCell>
                        <TableCell align="center">Object</TableCell>
                        <TableCell align="center">CreatedAt</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {rows.map((row) => (
                        <TableRow
                            key={row.WorkflowName}
                            sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                        >
                            <TableCell component="th" scope="row">
                                {row.WorkflowName}
                            </TableCell>
                            <TableCell align="center">{row.ForeignID}</TableCell>
                            <TableCell align="center">{row.RunID}</TableCell>
                            <TableCell align="center" className={`${row.IsEnd ? 'is-complete' : 'is-pending'}`}>{row.Status}</TableCell>
                            <TableCell align="center">
                                <StatusTimeline entries={myEntries}/>
                            </TableCell>
                            <TableCell align="center">{row.CreatedAt}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </TableContainer>
    );
}
