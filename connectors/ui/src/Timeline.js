import * as React from 'react';
import Timeline from '@mui/lab/Timeline';
import TimelineItem from '@mui/lab/TimelineItem';
import TimelineSeparator from '@mui/lab/TimelineSeparator';
import TimelineConnector from '@mui/lab/TimelineConnector';
import TimelineContent from '@mui/lab/TimelineContent';
import TimelineDot from '@mui/lab/TimelineDot';
import TimelineOppositeContent from '@mui/lab/TimelineOppositeContent';

export default function StatusTimeline() {
    return (
        <Timeline position="alternate">
            <TimelineItem>
                <TimelineOppositeContent color="text.secondary">
                    10:01
                </TimelineOppositeContent>
                <TimelineSeparator>
                    <TimelineDot />
                    <TimelineConnector />
                </TimelineSeparator>
                <TimelineContent>
                    Started
                </TimelineContent>
            </TimelineItem>

            <TimelineItem>
                <TimelineOppositeContent color="text.secondary">
                    10:02
                </TimelineOppositeContent>
                <TimelineSeparator>
                    <TimelineDot />
                    <TimelineConnector />
                </TimelineSeparator>
                <TimelineContent>
                    Processing
                </TimelineContent>
            </TimelineItem>

            <TimelineItem>
                <TimelineOppositeContent color="text.secondary">
                    10:03
                </TimelineOppositeContent>
                <TimelineSeparator>
                    <TimelineDot color="success" />
                </TimelineSeparator>
                <TimelineContent>Completed</TimelineContent>
            </TimelineItem>
        </Timeline>
    );
}