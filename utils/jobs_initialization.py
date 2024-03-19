import subprocess
import platform


def check_and_create_task_linux(script_path):
    # command to retrieve all cron jobs of the current user
    jobs = subprocess.run(["crontab", "-l"], capture_output=True, text=True)

    if script_path in jobs.stdout:
        print("Cron-Job already exists")
    else:
        print("Cron-Job will be created")
        new_job = f"0 * * * * /usr/bin/python3 {script_path}\n"
        all_jobs = jobs.stdout + new_job
        subprocess.run(["crontab", "-"], input=all_jobs, text=True)


def check_and_create_task_windows(task_name, script_path):
    # path to python interpreter needs to be changed accordingly!
    python_path = "C:\\ProgramData\\anaconda3\\python.exe"

    # check if task is already existing
    check_task = subprocess.run(["powershell", "-Command", f"Get-ScheduledTask -TaskName {task_name}"],
                                capture_output=True, text=True)

    if task_name in check_task.stdout:
        print("Task already exists")
    else:
        print("Task will be created")
        create_task = f'Schtasks /Create /SC HOURLY /TN "{task_name}" /TR "{python_path} {script_path}"'
        subprocess.run(["powershell", "-Command", create_task])


def main():
    """
    Main function to create job

    :return:
    """

    # path to python file needs to be changed accordingly!
    script_path = "C:\\Docs\\Studium\\Kurse\\#4DataQualityAndDataWrangling\\Code\\crawl.py"
    task_name = "data_crawl"  # change the task name (optional)

    os_system = platform.system()
    if os_system == "Linux":
        check_and_create_task_linux(script_path)
    elif os_system == "Windows":
        check_and_create_task_windows(task_name, script_path)
    else:
        print("OS not supported")


if __name__ == "__main__":
    main()
