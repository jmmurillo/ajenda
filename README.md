
# Ajenda  
  
## Introduction  
Ajenda is the Java implementation of a delayed message (here called "appointments") delivery system based on PostgreSQL 9.5+, supporting both one-shot and periodic appointments. It aims to be a lightweight, easy-to-configure solution for that kind of needs. Ajenda works by the simple mechanism of polling an appointments database table and fetching those whose execution date is (nearly) due.  
  
### Key characteristics  
 - Released as a Java library  
 - Based on PostgreSQL 9.5+ and JDBC.  
    - A version supporting Hibernate is planned  
 - Uses a periodic polling mechanism  
 - Supports one-shot delayed appointments  
 - Supports periodic appointments by fixed rate, fixed delay or by a Cront expression  
 - Supports concurrent producers and consumers  
    - Appointments can be divided in groups called "topics"  
    - Appointments are deleted once they are consumed  
    - Several error handling policies are available  
    - Simple clocks synchronization mechanism is provided  
  
## The appointments  
The appointments are the main objects handled in Ajenda. They are the messages to be delivered in the specified date/time.   
### One-shot appointments    
In its simplest form, an appointment consists on  
 - **Universally Unique Identifier (UUID)**: It must be unique in the **topic** domain.  
    - If an appointment with an already existing UUID is scheduled, then the previously existing appointment is updated with the new appointment information.  
    - The UUID of an appointment is necessary in case you may want to cancel it later, so an useful approach may be to use a hash of a more easily recallable identifier. An implementation of type-5 UUID is provided for such purpose.  
 - **Due timestamp**: The desired timestamp of delivery.  The system will try to deliver the appointment as close as possible to this date as possible, but never before.  
    - Stored as UNIX/Epoch time in milliseconds  
    - **UTC** is the only timezone currently supported.
 - **TTL**: Time-to-live of the appointment, after reaching its due date. Once TTL has passed, the appointment will be ignored
 - **Payload**: An arbitrary* string (or null) which contains the body of the message to deliver. **It is known that PostgreSQL does not allow the null character 0x00 in its strings*  
   
One-shots appointments must be created using `AppointmentBookingBuilder`. This class will provide several methods for setting the different parameters of the appointment:  
 - **UUID**  
    - `withUid(UUID appointmentUid)`: Explicitly sets the UUID of the appointment  
    - `withHashUid(String key)`: Builds a type-5 UUID from the given key (which uses the SHA-1 algorithm to build a hash).   
       - The same UUID can be reconstructed using `UUIDType5.nameUUIDFromCustomString(key)`  
    - `withHashUid()`: Builds a type-5 UUID from the payload and the due timestamp.  
       - The same UUID can be reconstructed using `UUIDType5.nameUUIDFromCustomString(payload + "_" + dueTimestamp)`  
 - **Due timestamp**  
    - `withDueTimestamp(long dueTimestamp)`: Explicitly sets the due timestamp of the appointment. Only positive numbers (post-epoch) are allowed  
    - `withDelayedDue(long delayMs)`: Sets a time of delay relative to the time of booking. Negative delays are not allowed  
    - `withImmediateDue()`: Sets the due timestamp equal to that of the booking. Equivalent to `withDelayedDue(0)`.   
    - If not specified, immediate due is the default  
    - Keep in mind that the reference time is that provided by the `Clock` of the `AjendaScheduler` used to book the appointment. By default, this will be the time provided by the PostgreSQL database in UTC, which may differ from the client system time.
 - **TTL**
	 - `withTtl(int ttlMs)`: Explicitly set the time-to-live period, in ms
 - **Payload**  
    - `withPayload(String payload)`: Explicitly sets the payload of the appointment  
      
### Periodic appointments  
As it may be expected, periodic appointments are much more complex than one-shot ones and consequently have more parameters to set. As an implementation detail which may be relevant, periodic appointments definitions are stored by topic but separated from the main appointment instances table. Then one-shot instances of the periodic appointment will be booked (roughly) periodically.  
In its simplest form, a periodic appointment consists on  
 - **Universally Unique Identifier (UUID)**: It must be unique in the **topic** domain among the periodic appointments. This UUID is for the **periodic appointment definition**, not for the one-shot appointment instances periodically spawned by it. As such, this will NOT collide with one-shot UUIDs. One-shot instances of a periodic appointment will have an UUID built by the expression `UUIDType5.nameUUIDFromCustomString(periodicAppointmentUid + "_" + dueTimestamp)`  
    - If a periodic appointment with an already existing UUID is scheduled, an exception will be thrown, in constrast with what happens with one-shot appointments.  
    - The UUID of a periodic appointment is necessary in case you may want to cancel it later, so an useful approach may be to use a hash of a more easily recallable identifier. An implementation of type-5 UUID is provided for such purpose.  
- **Time Pattern**: It specifies the periodicity of the appointment. Several types of patterns are supported:  
   - Fixed rate: Instances of the appointment will be booked with a fixed start-to-start period between them.  
   - Fixed delay: Instances of the appointment will be booked with a fixed end-to-start period between them.  
   - Cron expression: Instances of the appointment will be booked according to the periodicity given by the [Cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression), which can be interpeted in different flavours (Unix, Cron4J, Quartz or Spring).  
 - **Start timestamp**: The desired timestamp from which the periodic appointment will take place.  
    - Stored as UNIX/Epoch time in milliseconds  
    - **UTC** is the only timezone currently supported
 - **TTL**: Time-to-live of every iteration, after reaching their due date. Once TTL has passed, the iteration will be ignored
 - **Payload**: An arbitrary* string (or null) which contains the body of the message to deliver periodically. Every iteration will contain the same payload. **It is known that PostgreSQL does not allow the null character 0x00 in its strings*  
 - **Skip missed**: When set to *true* (default), iterations of this appointment whose due time are in the past will not be delivered. On the contrary, when set to *false*, every missed iteration will be delivered until the consumers catch up with the periodic appointment pace. Fixed delay appointments do not support this option. *For example, an hourly appointment is consumed at 0h, 1h and the consumer stops at 1:30h. If it resumes at 4:30h, an appointment which skips missed iterations will be delivered at 5h, 6h, etc. An appointment which does not skip missed iterations will deliver the iterations corresponding to 2h, 3h and 4h at the time of resuming (4:30h), then the ones corresponding to 5h, 6h, etc. at the expected time.*
 - **Key iteration**: Periodic appointments handling is simple but limited. Key iteration is a number N such that every N iterations a key iteration is added. Key iterations schedules the next N iterations, which implies that every poll you can only get a maximum of N iterations of that periodic appoinment. Furthermore, you wont get more iterations until the key iteration is processed. *It is therefore recommended to use an effective poll period smaller than (or equal to) the period of the periodic appointments.*
  
Periodic appointments must be created using `PeriodicAppointmentBookingBuilder`. This class will provide several methods for setting their different parameters:  
 - **UUID**  
    - `withUid(UUID appointmentUid)`: Explicitly sets the UUID of the appointment  
    - `withHashUid(String key)`: Builds a type-5 UUID from the given key (which uses the SHA-1 algorithm to build a hash).   
       - The same UUID can be reconstructed using `UUIDType5.nameUUIDFromCustomString(key)`  
    - `withHashUid()`: Builds a type-5 UUID from the payload and the time pattern.  
       - The same UUID can be reconstructed using `UUIDType5.nameUUIDFromCustomString(payload + "_" + patternType.name() + "_" + pattern)`   
 - **Time Pattern**  
    - `withFixedPeriod(long period, PeriodicPatternType patternType)`: For Fixed rate and Fixed delay appointments  
    - `withCronPattern(String pattern, PeriodicPatternType patternType)`: For Cron patterns  
 - **Start timestamp**  
    - `withDueTimestamp(long dueTimestamp)`: Explicitly sets the start timestamp of the appointment. Only positive numbers (post-epoch) are allowed  
    - `withDelayedStart(long delayMs)`: Sets a time of delay relative to the time of booking. Negative delays are not allowed  
    - `withImmediateStart()`: Sets the start timestamp equal to that of the booking. Equivalent to `withDelayedStart(0)`.   
    - If not specified, immediate start is the default 
 - **TTL**
	 - `withTtl(int ttlMs)`: Explicitly set the time-to-live period, in ms
 - **Payload**  
    - `withPayload(String payload)`: Explicitly sets the payload of the appointment  
 - **Skip missed**  
    - `withSkipMissed(boolean skipMissed)`: Whether skip missed iterations or not. Default is *true*
 - **Key iteration**  
    - `withKeyIteration(int keyIteration)`: Explicitly sets the gap between key iterations. Default is 1 and allows a maximum value of 1000.
